"""中亚班列货运履约台账的领域模型。

模型只描述结构与不变量，不负责落盘：

- 一笔订单（:class:`Order`）拆成可追踪的批次（:class:`Batch`），
  批次由一个或多个货位（:class:`Slot`）组成；
- 每个批次沿一条 :class:`Route`（路线版本）运输，按节点序列推进；
- 角色与可操作区段的归属集中在 :data:`ROLE_SEGMENTS`，便于鉴权复用。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Sequence

# ---------------------------------------------------------------------------
# 角色与区段
# ---------------------------------------------------------------------------


class Role(str, Enum):
    FORWARDER = "forwarder"          # 货代运营经理
    CARRIER = "carrier"              # 承运方（只能维护负责区段）
    PORT = "port"                    # 口岸查验人员
    SUPERVISOR = "supervisor"        # 值班主管（批准改线、拆并）
    CUSTOMER = "customer"            # 客户（只读脱敏进度）
    AUDITOR = "auditor"              # 审计人员（只读时间点还原）


# 班列沿线区段（南昌—霍尔果斯—比什凯克）。承运方按区段授权：
# 国内承运方负责南昌到霍尔果斯的境内段，口岸方负责口岸作业，
# 境外承运方负责换装后的哈/吉境内段。
class Segment(str, Enum):
    DOMESTIC = "domestic"            # 境内：南昌—霍尔果斯
    PORT = "port"                    # 霍尔果斯口岸作业
    OVERSEAS = "overseas"            # 境外：换装—比什凯克
    LAST_MILE = "last_mile"          # 比什凯克分拨与最终交付


# 每个角色允许写入的区段；未列入的角色不能推进任何节点。
ROLE_SEGMENTS: Mapping[Role, frozenset[Segment]] = {
    Role.CARRIER: frozenset({Segment.DOMESTIC, Segment.OVERSEAS, Segment.LAST_MILE}),
    Role.PORT: frozenset({Segment.PORT}),
}

# 节点到区段的归属。
NODE_SEGMENTS: Mapping[str, Segment] = {
    "booked": Segment.DOMESTIC,
    "loaded": Segment.DOMESTIC,       # 装箱
    "departed": Segment.DOMESTIC,
    "arrived_port": Segment.PORT,     # 抵达霍尔果斯
    "transship": Segment.PORT,       # 换装（准轨换宽轨）
    "inspection": Segment.PORT,       # 查验
    "released": Segment.PORT,         # 放行出境
    "departed_port": Segment.OVERSEAS,
    "abroad_transit": Segment.OVERSEAS,
    "arrived_almaty": Segment.OVERSEAS,  # 改线时可能经阿拉木图中转
    "arrived_bishkek": Segment.LAST_MILE,
    "split_dispatch": Segment.LAST_MILE,  # 分拨
    "delivered": Segment.LAST_MILE,   # 最终交付
}

# 正常履约路径的节点顺序；异常节点（detained/compensated）是旁路状态，
# 由事件本身携带语义，不占用主链游标。
ROUTE_NODES: Sequence[str] = (
    "booked",
    "loaded",
    "departed",
    "arrived_port",
    "transship",
    "inspection",
    "released",
    "departed_port",
    "abroad_transit",
    "arrived_bishkek",
    "split_dispatch",
    "delivered",
)

# 对客户可见的节点（异常与赔付明细不直接暴露，见 views 脱敏）。
CUSTOMER_VISIBLE_NODES: frozenset[str] = frozenset(ROUTE_NODES)


@dataclass(frozen=True)
class Route:
    """一条不可变的路线版本。

    改线不是覆盖旧路线，而是产生新版本号；审计可按时间点还原当时采用的版本。
    """

    version: int
    nodes: Sequence[str]
    note: str = ""
    effective_from_event: int | None = None  # 生效的事件序号（由服务回填）

    def index(self, node: str) -> int:
        try:
            return list(self.nodes).index(node)
        except ValueError as exc:
            raise ValueError(f"节点 {node} 不在路线 v{self.version} 上") from exc


DEFAULT_ROUTE = Route(version=1, nodes=ROUTE_NODES, note="南昌—霍尔果斯—比什凯克直达")


# ---------------------------------------------------------------------------
# 货位与批次
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Slot:
    """货位：订单内最小的可追踪物理单元。

    箱号（container_no）是口岸/承运方回传消息里的稳定业务编号；
    货位编号变化或毛重变化都视为"同编号但内容变化"，需要挂起核对。
    """

    slot_no: str
    container_no: str
    weight_kg: float
    goods: str

    def same_identity(self, other: "Slot") -> bool:
        return self.slot_no == other.slot_no and self.container_no == other.container_no


# 费用台账条目类型
FEE_FREIGHT = "freight_reserve"      # 运费预占
FEE_DEPOSIT = "release_deposit"      # 放行保证金
FEE_COMPENSATION = "compensation"    # 异常赔付


@dataclass
class Batch:
    """批次：同一路线、同一运输节奏下的一组货位。

    批次可被拆分或合并（需要主管批准），批次号沿用 + 生成新批次。
    """

    batch_no: str
    slots: list[Slot] = field(default_factory=list)
    route_version: int = 1
    node: str = "booked"
    active: bool = True          # False 表示已并入其他批次或交付关闭

    def slot(self, slot_no: str) -> Slot | None:
        return next((s for s in self.slots if s.slot_no == slot_no), None)
