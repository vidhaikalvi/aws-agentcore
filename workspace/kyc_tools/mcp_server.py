from mcp.server.fastmcp import FastMCP
from kyc_data_tools import KYCQueryEngine
import os
from pathlib import Path

MCP = FastMCP(host="127.0.0.1", port=8000, stateless_http=True)

KYC_DATA_PATH = os.environ.get("KYC_DATA_PATH", "./synthetic_data")

KYC_INDEXED_DATA: dict[str, KYCQueryEngine] = {}

KYC_INDEX_FIELDS = {
    "credit_reports": {
        "unique_id": "government_id",
        "text_fields": ["full_legal_name", "primary_address"],
    },
    "income_verification": {
        "unique_id": "government_id",
        "text_fields": ["employee_name"],
    },
    "property_records": {
        "unique_id": "property_id",
        "text_fields": ["owner_name_on_deed", "property_address"],
    },
    "lien_records": {
        "unique_id": "lien_id",
        "text_fields": ["debtor_name", "debtor_address"],
    },
}


def get_kyc_data_index():

    kyc_index_data = {}

    kyc_data_files = Path(KYC_DATA_PATH).glob("synthetic_*.json")

    if not kyc_data_files:
        raise FileNotFoundError(f"No KYC data files found in path: {KYC_DATA_PATH}")

    for file in kyc_data_files:
        file_name = file.stem
        display_name = file_name.replace("synthetic_", "").split(".")[0]
        kyc_index_data[display_name] = KYCQueryEngine.from_json_lines(
            file_path=file,
            unique_id_field=KYC_INDEX_FIELDS[display_name]["unique_id"],
            text_fields_to_index=KYC_INDEX_FIELDS[display_name]["text_fields"],
        )

    return kyc_index_data


@MCP.tool()
def credit_report_search(query: str, query_field: str, top_n: int = 3) -> list:
    """Search credit reports for relevant entries based on a query.

    Args:
        query (str): The search query.
        query_field (str): The field to query against ('full_legal_name', 'primary_address').
        top_n (int, optional): The number of top results to return. Defaults to 3.

    Returns:
        list: A list of relevant credit report entries.
    """
    results = KYC_INDEXED_DATA["credit_reports"].query_bm25(
        text=query, query_field=query_field, top_n=top_n
    )
    return results


@MCP.tool()
def income_verification_search(employee_name: str, top_n: int = 3) -> list:
    """Search income verification records for relevant entries based on a query.

    Args:
        employee_name (str): The employee's name to search for.
        top_n (int, optional): The number of top results to return. Defaults to 3.

    Returns:
        list: A list of relevant income verification entries.
    """
    results = KYC_INDEXED_DATA["income_verification"].query_bm25(
        text=employee_name, query_field="employee_name", top_n=top_n
    )
    return results


@MCP.tool()
def property_records_search(query: str, query_field: str, top_n: int = 3) -> list:
    """Search property records for relevant entries based on a query.

    Args:
        query (str): The search query.
        query_field (str): The field to query against ('owner_name_on_deed', 'property_address').
        top_n (int, optional): The number of top results to return. Defaults to 3.

    Returns:
        list: A list of relevant property record entries.
    """
    results = KYC_INDEXED_DATA["property_records"].query_bm25(
        text=query, query_field=query_field, top_n=top_n
    )
    return results


@MCP.tool()
def lien_records_search(query: str, query_field: str, top_n: int = 3) -> list:
    """Search lien records for relevant entries based on a query.

    Args:
        query (str): The search query.
        query_field (str): The field to query against ('debtor_name', 'debtor_address').
        top_n (int, optional): The number of top results to return. Defaults to 3.

    Returns:
        list: A list of relevant lien record entries.
    """
    results = KYC_INDEXED_DATA["lien_records"].query_bm25(
        text=query, query_field=query_field, top_n=top_n
    )
    return results


@MCP.tool()
def get_credit_report_by_id(government_id: str) -> dict:
    """Retrieve a credit report by government ID.

    Args:
        government_id (str): The government ID to search for.

    Returns:
        dict: The credit report entry if found, else an empty dictionary.
    """
    result = KYC_INDEXED_DATA["credit_reports"].query_unique_id(
        unique_id_value=government_id
    )
    return result if result else {}


@MCP.tool()
def get_income_verification_by_id(government_id: str) -> dict:
    """Retrieve an income verification record by government ID.

    Args:
        government_id (str): The government ID to search for.

    Returns:
        dict: The income verification entry if found, else an empty dictionary.
    """
    result = KYC_INDEXED_DATA["income_verification"].query_unique_id(
        unique_id_value=government_id
    )
    return result if result else {}


@MCP.tool()
def get_property_record_by_id(property_id: str) -> dict:
    """Retrieve a property record by property ID.

    Args:
        property_id (str): The property ID to search for.

    Returns:
        dict: The property record entry if found, else an empty dictionary.
    """
    result = KYC_INDEXED_DATA["property_records"].query_unique_id(
        unique_id_value=property_id
    )
    return result if result else {}


@MCP.tool()
def get_lien_record_by_id(lien_id: str) -> dict:
    """Retrieve a lien record by lien ID.

    Args:
        lien_id (str): The lien ID to search for.

    Returns:
        dict: The lien record entry if found, else an empty dictionary.
    """
    result = KYC_INDEXED_DATA["lien_records"].query_unique_id(unique_id_value=lien_id)
    return result if result else {}


if __name__ == "__main__":

    KYC_INDEXED_DATA = get_kyc_data_index()

    MCP.run(transport="streamable-http")
