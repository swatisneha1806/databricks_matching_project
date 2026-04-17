# Rules ordered NARROWEST → BROADEST: most filter anchors first, fewest last.
# Within the same anchor count, stricter predicates first: equals > fuzzy > startsWith > fullText > containsWordStartingWith
#
#  Rule1  : fuzzy(DBA|Name) + City + Country + Postal + Street  [5]  ← narrowest
#  Rule2  : exact(DBA|Name) + City + State + Country            [4, exact name]
#  Rule3  : fuzzy(DBA|Name) + City + State + Country            [4, fuzzy name]
#  Rule4  : exact(DBA)|fuzzy(Name) + City + Country             [3, name+city+country]
#  Rule5  : fuzzy(Name)     + City + Country                    [3, no-DBA entities]
#  Rule6  : fuzzy(DBA|Name) + State + Country                   [3, name+state+country]
#  Rule7  : fuzzy(DBA|Name) + Postal + Country                  [3, name+postal+country]
#  Rule8  : fuzzy(DBA)      + Postal + Country                  [3, DBA-only lower threshold]
#  Rule9  : fuzzy(Street)   + City + Country                    [3, address-led]
#  Rule10 : exact(DBA|Name) + Country                           [2, exact name]
#  Rule11 : startsWith(DBA) + Country                           [2, prefix name]
#  Rule12 : fuzzy(DBA|Name) + Country                           [2, fuzzy DBA]
#  Rule13 : fullText(DBA)   + Country                           [2, token DBA]
#  Rule14 : fullText(Name)  + Country                           [2, token legal name]
#  Rule15 : containsWordStartingWith(DBA|Name) + Country        [2, prefix token]

SEARCH_FILTERS = [

    {
        "name": "Filter_Rule1",
        "description": "Fuzzy DBA or legal name + exact city + exact country + postal prefix + fuzzy street",
        "template": (
            "((fuzzy(attributes.DoingBusinessAsName,'{name}') or fuzzy(attributes.Name,'{name}')) and "
            "equals(attributes.Address.City,'{city}') and "
            "(equals(attributes.Address.Country,'{country}') or equals(attributes.CountryISO2,'{country_iso2}') and "
            "startsWith(attributes.Address.PostalCode.PostalCode,'{postal_code}') and "
            "fuzzy(attributes.Address.AddressLine1,'{street_address}'))"
        ),
        "score_threshold": 0.80
    },
    {
        "name": "Filter_Rule2",
        "description": "Exact DBA or legal name + contains city + exact state + exact country",
        "template": (
            "((equals(attributes.DoingBusinessAsName,'{name}') or equals(attributes.Name,'{name}')) and "
            "contains(attributes.Address.City,'{city}') and "
            "equals(attributes.Address.StateProvince,'{state}') and "
            "(equals(attributes.Address.Country,'{country}') or equals(attributes.CountryISO2,'{country_iso2}'))"
        ),
        "score_threshold": 0.80
    },
    {
        "name": "Filter_Rule3",
        "description": "Fuzzy DBA or legal name + exact city + exact state + exact country",
        "template": (
            "((fuzzy(attributes.DoingBusinessAsName,'{name}') or fuzzy(attributes.Name,'{name}')) and "
            "equals(attributes.Address.City,'{city}') and "
            "equals(attributes.Address.StateProvince,'{state}') and "
            "(equals(attributes.Address.Country,'{country}') or equals(attributes.CountryISO2,'{country_iso2}'))"
        ),
        "score_threshold": 0.80
    },
    {
        "name": "Filter_Rule4",
        "description": "Exact DBA or fuzzy legal name + startsWith city + exact country",
        "template": (
            "((equals(attributes.DoingBusinessAsName,'{name}') or fuzzy(attributes.Name,'{name}')) and "
            "startsWith(attributes.Address.City,'{city}') and "
            "(equals(attributes.Address.Country,'{country}') or equals(attributes.CountryISO2,'{country_iso2}'))"
        ),
        "score_threshold": 0.75
    },
    {
        "name": "Filter_Rule5",
        "description": "Fuzzy legal Name + exact city + exact country — entities without DBA set",
        "template": (
            "(fuzzy(attributes.Name,'{name}') and "
            "equals(attributes.Address.City,'{city}') and "
            "(equals(attributes.Address.Country,'{country}') or equals(attributes.CountryISO2,'{country_iso2}'))"
        ),
        "score_threshold": 0.75
    },
    {
        "name": "Filter_Rule6",
        "description": "Fuzzy DBA or legal name + exact state + exact country (no city)",
        "template": (
            "((fuzzy(attributes.DoingBusinessAsName,'{name}') or fuzzy(attributes.Name,'{name}')) and "
            "equals(attributes.Address.StateProvince,'{state}') and "
            "(equals(attributes.Address.Country,'{country}') or equals(attributes.CountryISO2,'{country_iso2}'))"
        ),
        "score_threshold": 0.75
    },
    {
        "name": "Filter_Rule7",
        "description": "Fuzzy DBA or legal name + postal code prefix + exact country",
        "template": (
            "((fuzzy(attributes.DoingBusinessAsName,'{name}') or fuzzy(attributes.Name,'{name}')) and "
            "startsWith(attributes.Address.PostalCode.PostalCode,'{postal_code}') and "
            "(equals(attributes.Address.Country,'{country}') or equals(attributes.CountryISO2,'{country_iso2}'))"
        ),
        "score_threshold": 0.75
    },
    {
        "name": "Filter_Rule8",
        "description": "Fuzzy DBA name + postal code prefix + exact country",
        "template": (
            "(fuzzy(attributes.DoingBusinessAsName,'{name}') and "
            "startsWith(attributes.Address.PostalCode.PostalCode,'{postal_code}') and "
            "(equals(attributes.Address.Country,'{country}') or equals(attributes.CountryISO2,'{country_iso2}'))"
        ),
        "score_threshold": 0.50
    },
    {
        "name": "Filter_Rule9",
        "description": "Fuzzy street address + exact city + exact country",
        "template": (
            "fuzzy(attributes.Address.AddressLine1,'{street_address}') and "
            "equals(attributes.Address.City,'{city}') and "
            "(equals(attributes.Address.Country,'{country}') or equals(attributes.CountryISO2,'{country_iso2}')"
        ),
        "score_threshold": 0.50
    },
    {
        "name": "Filter_Rule10",
        "description": "Exact DBA or legal name + exact country",
        "template": (
            "((equals(attributes.DoingBusinessAsName,'{name}') or equals(attributes.Name,'{name}')) "
            "and (equals(attributes.Address.Country,'{country}') or equals(attributes.CountryISO2,'{country_iso2}'))"
        ),
        "score_threshold": 0.70
    },
    {
        "name": "Filter_Rule11",
        "description": "startsWith DBA name + exact country — long name stored short in DBA",
        "template": "(startsWith(attributes.DoingBusinessAsName,'{name}') "
                    "and (equals(attributes.Address.Country,'{country}') or equals(attributes.CountryISO2,'{country_iso2}'))",
        "score_threshold": 0.75
    },
    {
        "name": "Filter_Rule12",
        "description": "Fuzzy DBA name + exact country",
        "template": "((fuzzy(attributes.DoingBusinessAsName,'{name}') or fuzzy(attributes.Name,'{name}')) "
                    "and (equals(attributes.Address.Country,'{country}') or equals(attributes.CountryISO2,'{country_iso2}'))",
        "score_threshold": 0.0
    },
    {
        "name": "Filter_Rule13",
        "description": "fullText DBA name + exact country",
        "template": "(fullText(attributes.DoingBusinessAsName,'{name}') "
                    "and (equals(attributes.Address.Country,'{country}') or equals(attributes.CountryISO2,'{country_iso2}'))",
        "score_threshold": 0.0
    },
    {
        "name": "Filter_Rule14",
        "description": "fullText legal Name + exact country — entities without DBA set",
        "template": "(fullText(attributes.Name,'{name}') "
                    "and (equals(attributes.Address.Country,'{country}') or equals(attributes.CountryISO2,'{country_iso2}'))",
        "score_threshold": 0.0
    },
    {
        "name": "Filter_Rule15",
        "description": "containsWordStartingWith DBA or legal name + exact country",
        "template": (
            "((containsWordStartingWith(attributes.DoingBusinessAsName,'{name}') or "
            "containsWordStartingWith(attributes.Name,'{name}')) and "
            "(equals(attributes.Address.Country,'{country}') or equals(attributes.CountryISO2,'{country_iso2}'))"
        ),
        "score_threshold": 0.0
    },
]


BROAD_RULES = frozenset({
    "Filter_Rule10",  # exact name + country only
    "Filter_Rule11",  # startsWith DBA + country
    "Filter_Rule12",  # fuzzy DBA|Name + country
    "Filter_Rule13",  # fullText DBA + country
    "Filter_Rule14",  # fullText Name + country
    "Filter_Rule15",  # containsWordStartingWith DBA|Name + country
})


STATE_MAP = {
    "AL": "ALABAMA",
    "AK": "ALASKA",
    "AZ": "ARIZONA",
    "AR": "ARKANSAS",
    "CA": "CALIFORNIA",
    "CO": "COLORADO",
    "CT": "CONNECTICUT",
    "DE": "DELAWARE",
    "FL": "FLORIDA",
    "GA": "GEORGIA",
    "HI": "HAWAII",
    "ID": "IDAHO",
    "IL": "ILLINOIS",
    "IN": "INDIANA",
    "IA": "IOWA",
    "KS": "KANSAS",
    "KY": "KENTUCKY",
    "LA": "LOUISIANA",
    "ME": "MAINE",
    "MD": "MARYLAND",
    "MA": "MASSACHUSETTS",
    "MI": "MICHIGAN",
    "MN": "MINNESOTA",
    "MS": "MISSISSIPPI",
    "MO": "MISSOURI",
    "MT": "MONTANA",
    "NE": "NEBRASKA",
    "NV": "NEVADA",
    "NH": "NEW HAMPSHIRE",
    "NJ": "NEW JERSEY",
    "NM": "NEW MEXICO",
    "NY": "NEW YORK",
    "NC": "NORTH CAROLINA",
    "ND": "NORTH DAKOTA",
    "OH": "OHIO",
    "OK": "OKLAHOMA",
    "OR": "OREGON",
    "PA": "PENNSYLVANIA",
    "RI": "RHODE ISLAND",
    "SC": "SOUTH CAROLINA",
    "SD": "SOUTH DAKOTA",
    "TN": "TENNESSEE",
    "TX": "TEXAS",
    "UT": "UTAH",
    "VT": "VERMONT",
    "VA": "VIRGINIA",
    "WA": "WASHINGTON",
    "WV": "WEST VIRGINIA",
    "WI": "WISCONSIN",
    "WY": "WYOMING",
    # Canada Provinces
    "AB": "ALBERTA",
    "BC": "BRITISH COLUMBIA",
    "MB": "MANITOBA",
    "NB": "NEW BRUNSWICK",
    "NL": "NEWFOUNDLAND AND LABRADOR",
    "NS": "NOVA SCOTIA",
    "NT": "NORTHWEST TERRITORIES",
    "NU": "NUNAVUT",
    "ON": "ONTARIO",
    "PE": "PRINCE EDWARD ISLAND",
    "QC": "QUEBEC",
    "SK": "SASKATCHEWAN",
    "YT": "YUKON"
}
