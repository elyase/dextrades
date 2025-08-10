import dextrades


def test_import_and_client_creation():
    # Use public endpoints; no network calls are made on construction
    urls = [
        "https://eth-pokt.nodies.app",
        "https://ethereum.publicnode.com",
    ]
    client = dextrades.Client(urls)
    # get_stats is synchronous and does not hit network
    stats = client.get_stats()
    assert isinstance(stats, dict)
    assert {"events_extracted", "events_enriched"}.issubset(stats.keys())

