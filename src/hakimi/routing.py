"""Server-owned Decisions result. Strings remain compatible with business dispatch."""

from .security import Rejected

TIERS = ("tier_1_low", "tier_2_medium", "tier_3_high", "tier_4_carbon")


class RouteDecision(str):
    def __new__(
        cls,
        intent="chat",
        *,
        tier=2,
        sandbox_mode=False,
        need_memory_recall=False,
        need_external_lookup=False,
        module_query=None,
        reply_score=None,
        reply_threshold=None,
    ):
        value = super().__new__(cls, intent)
        value.tier = tier
        value.sandbox_mode = sandbox_mode
        value.need_memory_recall = need_memory_recall
        value.need_external_lookup = need_external_lookup
        value.module_query = module_query
        value.reply_score = reply_score
        value.reply_threshold = reply_threshold
        return value


def select_tier(probabilities, *, has_images=False, recall=False, text_length=0):
    if set(probabilities) != set(TIERS):
        raise Rejected("invalid_decision")
    ps = [probabilities[t] for t in TIERS]
    if any(isinstance(p, bool) or not isinstance(p, (int, float)) or not 0 <= p <= 1 for p in ps):
        raise Rejected("invalid_decision")
    if not 0.98 <= sum(ps) <= 1.02:
        raise Rejected("invalid_decision")
    tier = 4 if ps[3] >= 0.42 else 3 if sum(ps[2:]) >= 0.48 else 2 if sum(ps[1:]) >= 0.45 else 1
    return max(tier, 2) if text_length > 280 else tier
