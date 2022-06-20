class AtlasAPIConstants:
    """
    The Altana Atlas API Constants
    """

    # Node Types
    COMPANY_NODE = "company"
    FACILITY_NODE = "facility"
    ADDRESS_NODE = "address"
    DENIED_PARTY_NODE = "denied_party"
    NODE_TYPES = [COMPANY_NODE, FACILITY_NODE, ADDRESS_NODE, DENIED_PARTY_NODE]

    # Edge Types
    COMPANY_SENDS_TO_EDGE = "company_sends_to"
    COMPANY_SENDS_TO_SUMMARIZED_EDGE = "company_sends_to_summarized"
    FACILITY_SENDS_TO_EDGE = "facility_sends_to"
    FACILITY_SENDS_TO_SUMMARIZED_EDGE = "facility_sends_to_summarized"
    OPERATED_BY_EDGE = "operated_by"
    LOCATED_AT_EDGE = "located_at"
    OWNED_BY_EDGE = "owned_by"
    DENIED_PARTY_RELATED_TO_EDGE = "denied_party_related_to"
    EDGE_TYPES = [
        COMPANY_SENDS_TO_EDGE,
        COMPANY_SENDS_TO_SUMMARIZED_EDGE,
        FACILITY_SENDS_TO_EDGE,
        FACILITY_SENDS_TO_SUMMARIZED_EDGE,
        OPERATED_BY_EDGE,
        LOCATED_AT_EDGE,
        OWNED_BY_EDGE,
        DENIED_PARTY_RELATED_TO_EDGE,
    ]

    # Edge Traversals
    INBOUND = "inbound"
    OUTBOUND = "outbound"
    ANY = "any"
    EDGE_TRAVERSALS = [INBOUND, OUTBOUND, ANY]

    # Miscellaneous
    WILDCARD_EDGE_ID = "-"
