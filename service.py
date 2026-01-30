from __future__ import annotations

import ast
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException
from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session

from app.models.user import User

from .schema import (
    EstimateCreateIn,
    EstimateDetailOut,
    EstimateHistoryItemOut,
    EstimateListItemOut,
    EstimateSectionIn,
    EstimateUpdateIn,
)


def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except Exception:
        return None


def _money(v: Any) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _ensure_receiver_column(db: Session) -> None:
    """
    대표님 요구: 견적서 수신(발주처) 수정 가능.
    기존 DB에 receiver_name이 없을 수 있어, 없으면 안전하게 컬럼만 추가합니다.
    """
    exists = db.execute(
        text(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'estimates' AND column_name = 'receiver_name'
            LIMIT 1
            """
        )
    ).scalar()
    if not exists:
        db.execute(text("ALTER TABLE public.estimates ADD COLUMN receiver_name text"))
        db.commit()


def _get_project(db: Session, project_id: int) -> Dict[str, Any]:
    row = db.execute(
        text(
            """
            SELECT p.id, p.name, p.client_id, p.department_id,
                   p.start_date, p.created_at,
                   c.name AS client_name
            FROM projects p
            LEFT JOIN clients c ON c.id = p.client_id
            WHERE p.id = :pid
            """
        ),
        {"pid": project_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="프로젝트를 찾을 수 없습니다.")
    return dict(row)


def _generate_estimate_no(db: Session) -> Tuple[int, str]:
    """
    estimate id/estimate_no 생성(estimate_no NOT NULL 대응).
    - id는 estimates_id_seq(nextval)로 생성
    - estimate_no는 EST-YYYY-000001 형식(YYYY=현재년도, 번호=ID 기반 6자리)
    """
    new_id = db.execute(text("SELECT nextval('public.estimates_id_seq')")).scalar()
    if not new_id:
        raise HTTPException(status_code=500, detail="estimates_id_seq를 사용할 수 없습니다.")
    y = dt.datetime.now().year
    estimate_no = f"EST-{y}-{int(new_id):06d}"
    return int(new_id), estimate_no


# -----------------------------
# FORMULA 안전 평가
# -----------------------------
_ALLOWED_FORMULA_NODES = (
    ast.Expression,
    ast.BinOp,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.Pow,
    ast.UnaryOp,
    ast.UAdd,
    ast.USub,
    ast.Num,
    ast.Constant,
    ast.Name,
    ast.Load,
)


def _eval_formula(expr: str, env: Dict[str, float]) -> float:
    """
    아주 제한적으로만 FORMULA 평가.
    - 숫자/사칙연산/+ - * / 괄호만 허용
    - 함수 호출/속성 접근/인덱스 등 전부 차단
    """
    try:
        tree = ast.parse(expr, mode="eval")
    except Exception:
        raise HTTPException(status_code=400, detail=f"수식(formula) 파싱 실패: {expr}")

    for node in ast.walk(tree):
        # 허용 노드만 통과
        if not isinstance(node, _ALLOWED_FORMULA_NODES):
            raise HTTPException(status_code=400, detail="수식(formula)에서 허용되지 않는 구문이 포함되었습니다.")
        if isinstance(node, ast.Name) and node.id not in env:
            raise HTTPException(status_code=400, detail=f"수식(formula)에서 알 수 없는 변수: {node.id}")

    code = compile(tree, "<formula>", "eval")
    return float(eval(code, {"__builtins__": {}}, env))  # noqa: S307


def _recalc_sections(sections: List[EstimateSectionIn]) -> Tuple[Dict[str, float], float, float, float, List[Dict[str, Any]]]:
    """
    섹션/라인 계산(금액/소계/합계/부가세/총계) + 라인별 저장값 생성.
    - NORMAL: qty * unit_price
    - PERCENT_OF_SUBTOTAL: base_section_type(없으면 자기 섹션) 소계 * (qty/100)
    - FORMULA: 섹션 소계 변수 기반(MATERIAL, LABOR, EXPENSE, OVERHEAD, PROFIT, MANUAL)으로 제한 평가
    """
    if not sections:
        sections = [EstimateSectionIn(section_order=1, section_type="MANUAL", title="수동", lines=[])]

    subtotals: Dict[str, float] = {}
    serialized_lines: List[Dict[str, Any]] = []

    def _calc_normal(line) -> float:
        qty = float(line.qty or 0)
        unit_price = float(line.unit_price or 0)
        return qty * unit_price

    # 1차: NORMAL 기반 소계
    for sec in sections:
        st = 0.0
        for ln in sec.lines:
            if ln.calc_mode == "NORMAL":
                st += _calc_normal(ln)
        subtotals[sec.section_type] = st

    # 2차: 모든 라인 금액 산정 + 소계 재합산
    for sec in sections:
        st = 0.0
        for ln in sec.lines:
            if ln.calc_mode == "NORMAL":
                amt = _calc_normal(ln)
            elif ln.calc_mode == "PERCENT_OF_SUBTOTAL":
                base = ln.base_section_type or sec.section_type
                base_amt = float(subtotals.get(base, 0.0))
                amt = base_amt * (float(ln.qty or 0) / 100.0)
            elif ln.calc_mode == "FORMULA":
                env = {k: float(v) for k, v in subtotals.items()}
                expr = (ln.formula or "").strip()
                amt = _eval_formula(expr, env) if expr else 0.0
            else:
                amt = 0.0

            st += amt

            serialized_lines.append(
                {
                    "section_type": sec.section_type,
                    "section_order": sec.section_order,
                    "title": sec.title,
                    "line_order": ln.line_order,
                    "name": ln.name,
                    "spec": ln.spec,
                    "unit": ln.unit,
                    "qty": float(ln.qty or 0),
                    "unit_price": float(ln.unit_price or 0) if ln.unit_price is not None else None,
                    "amount": float(amt),
                    "remark": ln.remark,
                    "calc_mode": ln.calc_mode,
                    "base_section_type": ln.base_section_type,
                    "formula": ln.formula,
                    "source_type": ln.source_type,
                    "source_id": ln.source_id,
                    "price_type": ln.price_type,
                }
            )

        subtotals[sec.section_type] = st

    subtotal_all = float(sum(subtotals.values()))
    tax = float(round(subtotal_all * 0.10))
    total = float(subtotal_all + tax)
    return subtotals, subtotal_all, tax, total, serialized_lines


# -----------------------------
# 조회 API
# -----------------------------
def list_years(db: Session, *, business_state: Optional[str]) -> List[int]:
    where = ""
    params: Dict[str, Any] = {}
    if business_state:
        where = "WHERE e.business_state = :bs"
        params["bs"] = business_state

    rows = db.execute(
        text(
            f"""
            SELECT DISTINCT EXTRACT(YEAR FROM COALESCE(p.start_date, p.created_at, e.created_at))::int AS y
            FROM estimates e
            LEFT JOIN projects p ON p.id = e.project_id
            {where}
            ORDER BY y DESC
            """
        ),
        params,
    ).fetchall()

    years = [int(r[0]) for r in rows if r and r[0] is not None]
    if not years:
        cy = dt.datetime.now().year
        return [cy - i for i in range(0, 5)]
    return years


def list_estimates(
    db: Session,
    *,
    year: Optional[int],
    department_id: Optional[int],
    business_state: Optional[str],
    q: Optional[str],
) -> List[EstimateListItemOut]:
    wh: List[str] = ["e.deleted_at IS NULL"]
    params: Dict[str, Any] = {}

    if business_state:
        wh.append("e.business_state = :bs")
        params["bs"] = business_state

    if department_id:
        wh.append("p.department_id = :did")
        params["did"] = department_id

    if year:
        wh.append("EXTRACT(YEAR FROM COALESCE(p.start_date, p.created_at, e.created_at))::int = :y")
        params["y"] = year

    if q and q.strip():
        params["q"] = f"%{q.strip()}%"
        wh.append("(e.title ILIKE :q OR p.name ILIKE :q OR e.estimate_no ILIKE :q OR COALESCE(e.receiver_name,'') ILIKE :q)")

    where_sql = " AND ".join(wh)

    rows = db.execute(
        text(
            f"""
            SELECT
              e.id,
              e.estimate_no,
              e.project_id,
              p.name AS project_name,
              p.department_id,
              EXTRACT(YEAR FROM COALESCE(p.start_date, p.created_at, e.created_at))::int AS year,
              e.receiver_name,
              e.title,
              e.business_state,
              e.created_at,
              u.name AS author_name,
              r.subtotal,
              r.tax,
              r.total
            FROM estimates e
            LEFT JOIN projects p ON p.id = e.project_id
            LEFT JOIN users u ON u.id = e.created_by
            LEFT JOIN estimate_revisions r ON r.id = e.current_revision_id
            WHERE {where_sql}
            ORDER BY e.id DESC
            """
        ),
        params,
    ).mappings().all()

    out: List[EstimateListItemOut] = []
    for r in rows:
        out.append(
            EstimateListItemOut(
                id=int(r["id"]),
                estimate_no=r["estimate_no"],
                project_id=_safe_int(r.get("project_id")),
                project_name=r.get("project_name"),
                department_id=_safe_int(r.get("department_id")),
                year=_safe_int(r.get("year")),
                receiver_name=r.get("receiver_name"),
                title=r.get("title"),
                business_state=str(r.get("business_state") or "ONGOING"),
                created_at=str(r.get("created_at")),
                author_name=r.get("author_name"),
                subtotal=_money(r.get("subtotal")),
                tax=_money(r.get("tax")),
                total=_money(r.get("total")),
            )
        )
    return out


# -----------------------------
# 생성/수정(버전)
# -----------------------------
def _insert_sections_and_lines(
    db: Session,
    *,
    revision_id: int,
    sections: List[EstimateSectionIn],
) -> Tuple[float, float, float]:
    """
    - estimate_sections / estimate_items 저장
    - server-side 재계산 후 subtotal/tax/total 반환
    """
    if not sections:
        sections = [EstimateSectionIn(section_order=1, section_type="MANUAL", title="수동", lines=[])]

    subtotals, subtotal_all, tax, total, serialized_lines = _recalc_sections(sections)

    # 1) 섹션 생성(선택 순서 유지)
    section_id_map: Dict[Tuple[str, int], int] = {}
    for sec in sorted(sections, key=lambda s: s.section_order):
        sec_sub = float(subtotals.get(sec.section_type, 0.0))
        sid = db.execute(
            text(
                """
                INSERT INTO estimate_sections (revision_id, section_type, section_order, title, subtotal, created_at)
                VALUES (:rid, :stype, :sorder, :title, :subtotal, now())
                RETURNING id
                """
            ),
            {
                "rid": revision_id,
                "stype": sec.section_type,
                "sorder": sec.section_order,
                "title": sec.title,
                "subtotal": sec_sub,
            },
        ).scalar()
        section_id_map[(sec.section_type, sec.section_order)] = int(sid)

    # 1.5) product_id FK 안전장치: 프론트/검색 결과의 product_id가 DB(products)에 실제 존재하는지 확인
    # - 존재하지 않는 product_id가 들어오면 FK 위반으로 500 발생
    # - 스냅샷(제품명/규격/단가/수량)이 핵심이므로, 제품이 없으면 product_id는 NULL로 저장(수동항목처럼 처리)
    valid_product_ids: set[int] = set()
    product_ids = sorted({int(ln.get('source_id')) for ln in serialized_lines if ln.get('source_type') == 'PRODUCT' and ln.get('source_id') is not None})
    if product_ids:
        rows = db.execute(
            text('SELECT id FROM products WHERE id = ANY(:ids)'),
            {'ids': product_ids},
        ).fetchall()
        valid_product_ids = {int(r[0]) for r in rows}

    # 2) 라인 생성
    for ln in serialized_lines:
        sid = section_id_map.get((ln["section_type"], ln["section_order"])) or list(section_id_map.values())[0]

        # item_category는 기존 ENUM(MATERIAL/LABOR/EQUIPMENT/ETC)이라 확장 전까지 매핑한다.
        item_category = "ETC"
        if ln["section_type"] == "MATERIAL":
            item_category = "MATERIAL"
        elif ln["section_type"] == "LABOR":
            item_category = "LABOR"


        # FK 안전장치: products 테이블에 없는 product_id는 NULL로 저장(스냅샷은 유지)
        product_id = ln.get("source_id") if ln.get("source_type") == "PRODUCT" else None
        if product_id is not None and int(product_id) not in valid_product_ids:
            product_id = None

        db.execute(
            text(
                """
                INSERT INTO estimate_items (
                  revision_id,
                  line_no,
                  item_category,
                  product_id,
                  price_type,
                  item_name_snapshot,
                  spec_snapshot,
                  unit_snapshot,
                  unit_price_snapshot,
                  qty,
                  line_total,
                  memo,
                  section_id,
                  line_order,
                  calc_mode,
                  base_section_type,
                  formula
                )
                VALUES (
                  :rid,
                  :line_no,
                  CAST(:cat AS item_category),
                  :product_id,
                  COALESCE(CAST(:price_type AS price_type), 'MANUAL'::price_type),
                  :name,
                  :spec,
                  :unit,
                  :unit_price,
                  :qty,
                  :total,
                  :memo,
                  :section_id,
                  :line_order,
                  CAST(:calc_mode AS estimate_calc_mode),
                  CAST(:base_section_type AS estimate_section_type),
                  :formula
                )
                """
            ),
            {
                "rid": revision_id,
                "line_no": int(ln["line_order"]),
                "cat": item_category,
                "product_id": product_id,
                "price_type": ln.get("price_type") or "MANUAL",
                "name": ln["name"],
                "spec": ln.get("spec"),
                "unit": ln.get("unit") or "EA",
                "unit_price": float(ln.get("unit_price") or 0),
                "qty": float(ln.get("qty") or 0),
                "total": float(ln.get("amount") or 0),
                "memo": ln.get("remark"),
                "section_id": int(sid),
                "line_order": int(ln["line_order"]),
                "calc_mode": ln.get("calc_mode") or "NORMAL",
                "base_section_type": ln.get("base_section_type"),
                "formula": ln.get("formula"),
            },
        )

    return subtotal_all, tax, total


def create_estimate(db: Session, payload: EstimateCreateIn, current_user: User) -> Dict[str, Any]:
    _ensure_receiver_column(db)

    created_by = int(getattr(current_user, "id", 0) or 0)
    if not created_by:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    project = _get_project(db, int(payload.project_id))
    new_id, estimate_no = _generate_estimate_no(db)

    title = (payload.title or "").strip() or str(project.get("name") or "").strip() or "견적서"
    receiver_name = (payload.receiver_name or "").strip() or str(project.get("client_name") or "").strip() or None
    memo = (payload.memo or "").strip() or None

    db.execute(
        text(
            """
            INSERT INTO estimates (
              id, estimate_no, client_id, project_id, title, status, current_revision_id,
              created_by, memo, created_at, updated_at, receiver_name, business_state
            )
            VALUES (
              :id, :no, :client_id, :project_id, :title, 'DRAFT'::estimate_status, NULL,
              :created_by, :memo, now(), now(), :receiver_name, 'ONGOING'::estimate_business_state
            )
            """
        ),
        {
            "id": new_id,
            "no": estimate_no,
            "client_id": int(project["client_id"]),
            "project_id": int(project["id"]),
            "title": title,
            "created_by": created_by,
            "memo": memo,
            "receiver_name": receiver_name,
        },
    )

    rev_id = db.execute(
        text(
            """
            INSERT INTO estimate_revisions (estimate_id, revision_no, reason, subtotal, tax, total, status, created_by, created_at)
            VALUES (:eid, 1, NULL, 0, 0, 0, 'DRAFT'::revision_status, :created_by, now())
            RETURNING id
            """
        ),
        {"eid": new_id, "created_by": created_by},
    ).scalar()
    revision_id = int(rev_id)

    # 섹션 기반 저장(legacy_items는 Step3에서 제거 예정이지만 임시 호환)
    sections = payload.sections or []
    subtotal_all, tax, total = _insert_sections_and_lines(db, revision_id=revision_id, sections=sections)

    db.execute(text("UPDATE estimate_revisions SET subtotal=:s, tax=:t, total=:tt WHERE id=:rid"),
               {"s": subtotal_all, "t": tax, "tt": total, "rid": revision_id})
    db.execute(text("UPDATE estimates SET current_revision_id=:rid, updated_at=now() WHERE id=:eid"),
               {"rid": revision_id, "eid": new_id})

    db.commit()
    return {"id": new_id, "estimate_no": estimate_no}


def _get_estimate(db: Session, estimate_id: int) -> Dict[str, Any]:
    row = db.execute(
        text(
            """
            SELECT e.*, p.name AS project_name
            FROM estimates e
            LEFT JOIN projects p ON p.id = e.project_id
            WHERE e.id = :id AND e.deleted_at IS NULL
            """
        ),
        {"id": estimate_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="견적서를 찾을 수 없습니다.")
    return dict(row)


def _get_revision(db: Session, revision_id: int) -> Dict[str, Any]:
    row = db.execute(
        text(
            """
            SELECT r.*, u.name AS author_name
            FROM estimate_revisions r
            LEFT JOIN users u ON u.id = r.created_by
            WHERE r.id = :id
            """
        ),
        {"id": revision_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="견적서 버전을 찾을 수 없습니다.")
    return dict(row)


def get_estimate_detail(db: Session, estimate_id: int) -> EstimateDetailOut:
    e = _get_estimate(db, estimate_id)
    rev_id = e.get("current_revision_id")
    if not rev_id:
        raise HTTPException(status_code=500, detail="견적서 current_revision_id가 비어있습니다.")
    r = _get_revision(db, int(rev_id))

    sections = db.execute(
        text(
            """
            SELECT id, revision_id, section_type, section_order, title, subtotal
            FROM estimate_sections
            WHERE revision_id = :rid
            ORDER BY section_order ASC
            """
        ),
        {"rid": int(rev_id)},
    ).mappings().all()

    lines = db.execute(
        text(
            """
            SELECT
              id,
              section_id,
              COALESCE(line_order, line_no) AS line_order,
              item_name_snapshot AS name,
              spec_snapshot AS spec,
              unit_snapshot AS unit,
              qty,
              unit_price_snapshot AS unit_price,
              line_total AS amount,
              memo AS remark,
              calc_mode,
              base_section_type,
              formula,
              product_id,
              price_type
            FROM estimate_items
            WHERE revision_id = :rid
            ORDER BY section_id ASC, COALESCE(line_order, line_no) ASC, id ASC
            """
        ),
        {"rid": int(rev_id)},
    ).mappings().all()

    sec_map: Dict[int, List[Dict[str, Any]]] = {}
    for ln in lines:
        sid = int(ln["section_id"]) if ln.get("section_id") is not None else 0
        sec_map.setdefault(sid, []).append(dict(ln))

    out_sections = []
    for s in sections:
        sid = int(s["id"])
        out_lines = []
        for ln in sec_map.get(sid, []):
            out_lines.append(
                {
                    "id": int(ln["id"]),
                    "line_order": int(ln["line_order"] or 1),
                    "name": ln["name"],
                    "spec": ln.get("spec"),
                    "unit": ln.get("unit") or "EA",
                    "qty": _money(ln.get("qty")),
                    "unit_price": _money(ln.get("unit_price")),
                    "amount": _money(ln.get("amount")),
                    "remark": ln.get("remark"),
                    "calc_mode": str(ln.get("calc_mode") or "NORMAL"),
                    "base_section_type": ln.get("base_section_type"),
                    "formula": ln.get("formula"),
                    "source_type": "PRODUCT" if ln.get("product_id") else "NONE",
                    "source_id": _safe_int(ln.get("product_id")),
                    "price_type": ln.get("price_type"),
                }
            )

        out_sections.append(
            {
                "id": sid,
                "section_order": int(s.get("section_order") or 1),
                "section_type": s.get("section_type"),
                "title": s.get("title"),
                "subtotal": _money(s.get("subtotal")),
                "lines": out_lines,
            }
        )

    return EstimateDetailOut(
        id=int(e["id"]),
        estimate_no=e["estimate_no"],
        business_state=str(e.get("business_state") or "ONGOING"),
        project_id=_safe_int(e.get("project_id")),
        project_name=e.get("project_name"),
        receiver_name=e.get("receiver_name"),
        title=e.get("title"),
        memo=e.get("memo"),
        revision_id=int(rev_id),
        revision_no=int(r.get("revision_no") or 1),
        revision_status=str(r.get("status") or "DRAFT"),
        created_at=str(r.get("created_at")),
        author_name=r.get("author_name"),
        subtotal=_money(r.get("subtotal")),
        tax=_money(r.get("tax")),
        total=_money(r.get("total")),
        sections=out_sections,
    )


def update_estimate(db: Session, estimate_id: int, payload: EstimateUpdateIn, current_user: User) -> Dict[str, Any]:
    _ensure_receiver_column(db)

    created_by = int(getattr(current_user, "id", 0) or 0)
    if not created_by:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    e = _get_estimate(db, estimate_id)
    cur_rev_id = e.get("current_revision_id")
    if not cur_rev_id:
        raise HTTPException(status_code=500, detail="current_revision_id가 비어있습니다.")
    cur_rev = _get_revision(db, int(cur_rev_id))
    cur_no = int(cur_rev.get("revision_no") or 1)

    # 기존 revision LOCK
    db.execute(text("UPDATE estimate_revisions SET status='LOCKED'::revision_status WHERE id=:rid"), {"rid": int(cur_rev_id)})

    # 신규 revision
    new_rev_id = db.execute(
        text(
            """
            INSERT INTO estimate_revisions (estimate_id, revision_no, reason, subtotal, tax, total, status, created_by, created_at)
            VALUES (:eid, :no, :reason, 0, 0, 0, 'DRAFT'::revision_status, :created_by, now())
            RETURNING id
            """
        ),
        {"eid": estimate_id, "no": cur_no + 1, "reason": payload.reason, "created_by": created_by},
    ).scalar()
    new_rev_id = int(new_rev_id)

    sections = payload.sections or []
    subtotal_all, tax, total = _insert_sections_and_lines(db, revision_id=new_rev_id, sections=sections)

    db.execute(text("UPDATE estimate_revisions SET subtotal=:s, tax=:t, total=:tt WHERE id=:rid"),
               {"s": subtotal_all, "t": tax, "tt": total, "rid": new_rev_id})

    # estimates 헤더 업데이트 + current_revision_id 변경
    sets = ["current_revision_id = :rid", "updated_at = now()"]
    params: Dict[str, Any] = {"rid": new_rev_id, "eid": estimate_id}

    if payload.title is not None:
        sets.append("title = :title")
        params["title"] = payload.title.strip() or None

    if payload.receiver_name is not None:
        sets.append("receiver_name = :receiver_name")
        params["receiver_name"] = payload.receiver_name.strip() or None

    if payload.memo is not None:
        sets.append("memo = :memo")
        params["memo"] = payload.memo.strip() or None

    db.execute(text(f"UPDATE estimates SET {', '.join(sets)} WHERE id = :eid"), params)
    db.commit()
    return {"ok": True, "revision_id": new_rev_id}


def update_business_state(db: Session, estimate_id: int, business_state: str) -> Dict[str, Any]:
    _get_estimate(db, estimate_id)
    db.execute(text("UPDATE estimates SET business_state=:bs, updated_at=now() WHERE id=:id"),
               {"bs": business_state, "id": estimate_id})
    db.commit()
    return {"ok": True, "business_state": business_state}




def get_estimate_detail_by_revision(db: Session, estimate_id: int, revision_id: int) -> EstimateDetailOut:
    """
    특정 revision_id 기준으로 견적서 상세(섹션/라인 포함) 구성.
    - 권한/가시성 제한은 두지 않고, 데이터만 반환(프론트에서 '구버전 표시' 용도)
    """
    e = _get_estimate(db, estimate_id)
    r = _get_revision(db, int(revision_id))

    sections = db.execute(
        text(
            """
            SELECT id, revision_id, section_type, section_order, title, subtotal
            FROM estimate_sections
            WHERE revision_id = :rid
            ORDER BY section_order ASC
            """
        ),
        {"rid": int(revision_id)},
    ).mappings().all()

    lines = db.execute(
        text(
            """
            SELECT
              id,
              section_id,
              COALESCE(line_order, line_no) AS line_order,
              item_name_snapshot AS name,
              spec_snapshot AS spec,
              unit_snapshot AS unit,
              qty,
              unit_price_snapshot AS unit_price,
              line_total AS amount,
              memo AS remark,
              calc_mode,
              base_section_type,
              formula,
              product_id,
              price_type
            FROM estimate_items
            WHERE revision_id = :rid
            ORDER BY section_id ASC, COALESCE(line_order, line_no) ASC, id ASC
            """
        ),
        {"rid": int(revision_id)},
    ).mappings().all()

    sec_map: Dict[int, List[Dict[str, Any]]] = {}
    for ln in lines:
        sid = int(ln["section_id"]) if ln.get("section_id") is not None else 0
        sec_map.setdefault(sid, []).append(dict(ln))

    out_sections = []
    for s in sections:
        sid = int(s["id"])
        out_lines = []
        for ln in sec_map.get(sid, []):
            out_lines.append(
                {
                    "id": int(ln["id"]),
                    "line_order": int(ln["line_order"] or 1),
                    "name": ln["name"],
                    "spec": ln.get("spec"),
                    "unit": ln.get("unit") or "EA",
                    "qty": _money(ln.get("qty")),
                    "unit_price": _money(ln.get("unit_price")) if ln.get("unit_price") is not None else None,
                    "amount": _money(ln.get("amount")),
                    "remark": ln.get("remark"),
                    "calc_mode": str(ln.get("calc_mode") or "NORMAL"),
                    "base_section_type": ln.get("base_section_type"),
                    "formula": ln.get("formula"),
                    "source_type": "PRODUCT" if ln.get("product_id") else "NONE",
                    "source_id": _safe_int(ln.get("product_id")),
                    "price_type": ln.get("price_type"),
                }
            )

        out_sections.append(
            {
                "id": sid,
                "section_order": int(s.get("section_order") or 1),
                "section_type": s.get("section_type"),
                "title": s.get("title"),
                "subtotal": _money(s.get("subtotal")),
                "lines": out_lines,
            }
        )

    return EstimateDetailOut(
        id=int(e["id"]),
        estimate_no=e["estimate_no"],
        business_state=str(e.get("business_state") or "ONGOING"),
        project_id=_safe_int(e.get("project_id")),
        project_name=e.get("project_name"),
        receiver_name=e.get("receiver_name"),
        title=e.get("title"),
        memo=e.get("memo"),
        revision_id=int(revision_id),
        revision_no=int(r.get("revision_no") or 1),
        revision_status=str(r.get("status") or "DRAFT"),
        created_at=str(r.get("created_at")),
        author_name=r.get("author_name") or e.get("author_name"),
        subtotal=_money(r.get("subtotal")),
        tax=_money(r.get("tax")),
        total=_money(r.get("total")),
        sections=out_sections,
    )


def get_history_details(db: Session, estimate_id: int, limit: int = 10) -> List[EstimateDetailOut]:
    """
    구버전(이전 revision) 상세를 최근 N개(limit)까지 반환.
    - 최신(current_revision_id)은 제외하고, 나머지 revision을 revision_no DESC로 가져옴
    """
    e = _get_estimate(db, estimate_id)
    current_rid = _safe_int(e.get("current_revision_id"))

    rows = db.execute(
        text(
            """
            SELECT id AS revision_id
            FROM estimate_revisions
            WHERE estimate_id = :eid
            ORDER BY revision_no DESC
            """
        ),
        {"eid": estimate_id},
    ).mappings().all()

    rev_ids = [int(r["revision_id"]) for r in rows if r.get("revision_id") is not None]
    # 최신 제외
    if current_rid:
        rev_ids = [rid for rid in rev_ids if rid != int(current_rid)]

    rev_ids = rev_ids[: max(0, int(limit or 10))]

    out: List[EstimateDetailOut] = []
    for rid in rev_ids:
        out.append(get_estimate_detail_by_revision(db, estimate_id, rid))
    return out


def get_history(db: Session, estimate_id: int) -> List[EstimateHistoryItemOut]:
    _get_estimate(db, estimate_id)
    rows = db.execute(
        text(
            """
            SELECT r.id AS revision_id, r.revision_no, r.status, r.created_at, r.created_by,
                   u.name AS author_name, r.subtotal, r.tax, r.total
            FROM estimate_revisions r
            LEFT JOIN users u ON u.id = r.created_by
            WHERE r.estimate_id = :eid
            ORDER BY r.revision_no DESC
            """
        ),
        {"eid": estimate_id},
    ).mappings().all()

    out: List[EstimateHistoryItemOut] = []
    for r in rows:
        out.append(
            EstimateHistoryItemOut(
                revision_id=int(r["revision_id"]),
                revision_no=int(r["revision_no"]),
                status=str(r["status"]),
                created_at=str(r["created_at"]),
                created_by=int(r["created_by"]),
                author_name=r.get("author_name"),
                subtotal=_money(r.get("subtotal")),
                tax=_money(r.get("tax")),
                total=_money(r.get("total")),
            )
        )
    return out


# -----------------------------
# 삭제(관리자)
# -----------------------------
def delete_estimate_with_revisions(db: Session, estimate_id: int):
    eid = int(estimate_id)

    # FK: estimates.current_revision_id -> estimate_revisions.id
    # 먼저 참조를 끊어야 revision 삭제 가능
    db.execute(
        text("UPDATE estimates SET current_revision_id = NULL WHERE id = :eid"),
        {"eid": eid},
    )

    # 해당 estimate의 revision_id 목록
    rev_rows = db.execute(
        text("SELECT id FROM estimate_revisions WHERE estimate_id = :eid"),
        {"eid": eid},
    ).fetchall()
    rev_ids = [int(r[0]) for r in rev_rows]

    if rev_ids:
        # items 삭제 (revision_id 기준)
        db.execute(
            text("DELETE FROM estimate_items WHERE revision_id IN :rids").bindparams(
                bindparam("rids", expanding=True)
            ),
            {"rids": rev_ids},
        )

        # sections 삭제 (revision_id 기준)
        db.execute(
            text("DELETE FROM estimate_sections WHERE revision_id IN :rids").bindparams(
                bindparam("rids", expanding=True)
            ),
            {"rids": rev_ids},
        )

    # revisions 삭제
    db.execute(
        text("DELETE FROM estimate_revisions WHERE estimate_id = :eid"),
        {"eid": eid},
    )

    # estimates 삭제
    db.execute(
        text("DELETE FROM estimates WHERE id = :eid"),
        {"eid": eid},
    )

    db.commit()
