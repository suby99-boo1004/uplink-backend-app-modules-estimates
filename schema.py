from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, Field


SectionType = Literal["MATERIAL", "LABOR", "EXPENSE", "OVERHEAD", "PROFIT", "MANUAL"]
CalcMode = Literal["NORMAL", "PERCENT_OF_SUBTOTAL", "FORMULA"]
BusinessState = Literal["ONGOING", "DONE", "CANCELED"]


class EstimateLineIn(BaseModel):
    """견적서 표의 한 줄(라인) 입력"""

    line_order: int = Field(default=1, description="섹션 내 표시 순서(1..N)")
    name: str = Field(..., description="제품명/항목명")
    spec: Optional[str] = Field(default=None, description="규격/설명")
    unit: str = Field(default="EA", description="단위(EA, 식, %, ...)")
    qty: float = Field(default=1, description="수량 또는 % 값(예: 3.7)")
    unit_price: Optional[float] = Field(default=None, description="단가(원). %라인은 공란 가능")
    amount: Optional[float] = Field(default=None, description="금액(원). 서버에서 재계산 가능")
    remark: Optional[str] = Field(default=None, description="비고/메모")

    # 연동/계산 정보
    calc_mode: CalcMode = Field(default="NORMAL", description="계산 모드")
    base_section_type: Optional[SectionType] = Field(default=None, description="%/FORMULA 기준 섹션 타입")
    formula: Optional[str] = Field(default=None, description="FORMULA 표현식(서버에서 제한적으로 평가)")

    # 원본 연결(제품/일위대가 등)
    source_type: Optional[Literal["PRODUCT", "LABOR_ITEM", "NONE"]] = Field(default="NONE")
    source_id: Optional[int] = None
    price_type: Optional[Literal["DESIGN", "CONSUMER", "SUPPLY", "MANUAL"]] = Field(default=None)


class EstimateSectionIn(BaseModel):
    """견적서 섹션(재료비/노무비/...) 입력"""

    section_order: int = Field(default=1, description="견적서 내 섹션 표시 순서(선택한 순서)")
    section_type: SectionType
    title: str = Field(..., description="섹션 제목(예: 재료비)")
    lines: List[EstimateLineIn] = Field(default_factory=list)


class EstimateCreateIn(BaseModel):
    """신규 견적서 생성(권장: 섹션 기반)"""

    project_id: int = Field(..., description="진행 프로젝트 ID(필수)")
    title: Optional[str] = Field(default=None, description="견적서 제목(미입력 시 프로젝트명 사용)")
    receiver_name: Optional[str] = Field(default=None, description="수신(발주처) - 수정 가능")
    memo: Optional[str] = Field(default=None, description="비고(헤더 메모)")

    sections: List[EstimateSectionIn] = Field(default_factory=list, description="섹션 기반 입력(권장)")

    # legacy 호환: 기존 프론트가 라인 단위로 보내는 경우
    legacy_items: Optional[List[dict]] = Field(default=None, description="(임시) 기존 라인 기반 입력")


class EstimateUpdateIn(BaseModel):
    """견적서 수정 저장(=신규 revision 생성)"""

    title: Optional[str] = None
    receiver_name: Optional[str] = None
    memo: Optional[str] = None
    reason: Optional[str] = Field(default=None, description="개정 사유(선택)")

    sections: List[EstimateSectionIn] = Field(default_factory=list)
    legacy_items: Optional[List[dict]] = None


class EstimateListItemOut(BaseModel):
    id: int
    estimate_no: str
    project_id: Optional[int] = None
    project_name: Optional[str] = None
    department_id: Optional[int] = None
    year: Optional[int] = None

    receiver_name: Optional[str] = None
    title: Optional[str] = None

    business_state: BusinessState
    created_at: str
    author_name: Optional[str] = None

    subtotal: float = 0
    tax: float = 0
    total: float = 0


class EstimateLineOut(BaseModel):
    id: int
    line_order: int
    name: str
    spec: Optional[str] = None
    unit: str
    qty: float
    unit_price: Optional[float] = None
    amount: float
    remark: Optional[str] = None
    calc_mode: CalcMode
    base_section_type: Optional[SectionType] = None
    formula: Optional[str] = None
    source_type: Optional[str] = None
    source_id: Optional[int] = None
    price_type: Optional[str] = None


class EstimateSectionOut(BaseModel):
    id: int
    section_order: int
    section_type: SectionType
    title: str
    subtotal: float
    lines: List[EstimateLineOut]


class EstimateDetailOut(BaseModel):
    id: int
    estimate_no: str
    business_state: BusinessState

    project_id: Optional[int] = None
    project_name: Optional[str] = None
    receiver_name: Optional[str] = None
    title: Optional[str] = None
    memo: Optional[str] = None

    revision_id: int
    revision_no: int
    revision_status: str
    created_at: str
    author_name: Optional[str] = None

    subtotal: float
    tax: float
    total: float

    sections: List[EstimateSectionOut] = Field(default_factory=list)


class EstimateHistoryItemOut(BaseModel):
    revision_id: int
    revision_no: int
    status: str
    created_at: str
    created_by: int
    author_name: Optional[str] = None
    subtotal: float
    tax: float
    total: float


class EstimateStatusUpdateIn(BaseModel):
    business_state: BusinessState
