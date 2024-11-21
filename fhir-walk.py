#!/usr/bin/env python3

import requests, sys, json
from urllib.parse import urlparse


def fixup_url(url, base_url):
    """
    Replace FHIR base URL in given FHIR API call with different base_url
    Helpful when a FHIR server is configured with a server_name that does not match its public one
    No generalized solution when urls significantly different
    """
    base_scheme = urlparse(base_url).scheme
    configured_scheme = urlparse(url).scheme
    if base_scheme != configured_scheme:
        url = url.replace(configured_scheme, base_scheme)

    domain = urlparse(url).netloc
    base_domain = urlparse(base_url).netloc
    url = url.replace(domain, base_domain)
    return url


def get_page(url, fhir_base_url):
    url = fixup_url(url, fhir_base_url)

    results = requests.get(url)
    results.raise_for_status()
    results = results.json()
    yield results

    for link in results["link"]:
        if link["relation"] == "next":
            yield from get_page(link["url"], fhir_base_url)


def get_resources(resource_name, fhir_base_url, output_dir):
    url = fixup_url(
        url=f"{fhir_base_url}/{resource_name}",
        base_url=fhir_base_url,
    )
    for page in get_page(url, fhir_base_url):
        with open(f"{output_dir}/{resource_name}.ndjson", "a") as resource_export_ndjson:
            for entry in page["entry"]:
                resource_json = json.dumps(entry["resource"])
                resource_export_ndjson.write(resource_json)
                resource_export_ndjson.write("\n")


def get_counts(supported_resources, fhir_base_url):
    # TODO use coroutines
    resource_counts = {}
    for resource in supported_resources:
        summary = requests.get(f"{fhir_base_url}/{resource}", params={"_summary": "count"})
        resource_counts[resource] = summary.json()["total"]
    return resource_counts


def main():
    fhir_base_url = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "./"

    print("Getting supported resource types...")
    metadata = requests.get(f"{fhir_base_url}/metadata")
    metadata.json()["rest"][0]["resource"]
    supported_resources = {r["type"] for r in metadata.json()["rest"][0]["resource"]}

    print("Getting resource counts...")
    resource_counts = get_counts(supported_resources, fhir_base_url)
    print(resource_counts)

    print(f"Writing {sum(resource_counts.values())} resources from {fhir_base_url} to {output_dir}...")
    for resource_name, resource_count in resource_counts.items():
        if resource_count == 0:
            continue
        print(f"Writing {resource_name} resources...")
        get_resources(resource_name, fhir_base_url, output_dir)

    print("Finished downloading all resources")


if __name__ == "__main__":
    main()
