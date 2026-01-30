from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.deps import get_db, get_current_user
from app.models.user import User

from .schema import (
    EstimateCreateIn,
    EstimateDetailOut,
    EstimateHistoryItemOut,
    EstimateListItemOut,
    EstimateStatusUpdateIn,
    EstimateUpdateIn,
)
from .service import (
    create_estimate,
    get_estimate_detail,
    get_history,
    get_history_details,
    list_estimates,
    list_years,
    update_business_state,
    update_estimate,
)

router = APIRouter(prefix="/api/estimates", tags=["견적서"])


@router.get("/ping")
def ping():
    return {"ok": True, "module": "estimates"}


@router.get("/years")
def api_years(
    status: Optional[str] = Query(default=None, description="ONGOING|DONE|CANCELED"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    years = list_years(db, business_state=status)
    return {"status": status, "years": years}


@router.get("", response_model=List[EstimateListItemOut])
def api_list(
    year: Optional[int] = Query(default=None),
    department_id: Optional[int] = Query(default=None),
    status: Optional[str] = Query(default=None, description="ONGOING|DONE|CANCELED"),
    q: Optional[str] = Query(default=None, description="사업명/견적번호/수신/제목 검색"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return list_estimates(db, year=year, department_id=department_id, business_state=status, q=q)




@router.get("/{estimate_id}/history-details", response_model=List[EstimateDetailOut])
def api_history_details(
    estimate_id: int,
    limit: int = Query(default=10, ge=1, le=10),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # 모든 로그인 사용자에게 동일하게 제공(권한 제한 없음)
    return get_history_details(db, estimate_id, limit=limit)

@router.post("", response_model=dict)
def api_create(
    payload: EstimateCreateIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # 1프로젝트=1견적: 이미 해당 프로젝트로 견적서가 있으면 신규등록 차단
    exists = db.execute(
        text(
            """
            SELECT 1
            FROM estimates
            WHERE project_id = :pid
              AND (deleted_at IS NULL)
            LIMIT 1
            """
        ),
        {"pid": int(payload.project_id)},
    ).first()
    if exists:
        raise HTTPException(status_code=400, detail="해당 프로젝트는 이미 견적서가 생성되어 있습니다.")
    return create_estimate(db, payload, current_user)


@router.get("/{estimate_id}", response_model=EstimateDetailOut)
def api_detail(
    estimate_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return get_estimate_detail(db, estimate_id)


@router.put("/{estimate_id}", response_model=dict)
def api_update(
    estimate_id: int,
    payload: EstimateUpdateIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return update_estimate(db, estimate_id, payload, current_user)


@router.get("/{estimate_id}/history", response_model=List[EstimateHistoryItemOut])
def api_history(
    estimate_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return get_history(db, estimate_id)


@router.post("/{estimate_id}/business-state", response_model=dict)
def api_business_state(
    estimate_id: int,
    payload: EstimateStatusUpdateIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return update_business_state(db, estimate_id, payload.business_state)
