import httpx
import asyncio
from datetime import datetime, timedelta

class AsyncPrometheusQueryExecutor:
    def __init__(self, prometheus_base_url, request_timeout=10.0):
        """
        Initialize the class with the base URL of the Prometheus server.
        :param prometheus_base_url: URL of the Prometheus server.
        :param request_timeout: Timeout for the Prometheus query request in seconds.
        """
        self.prometheus_base_url = prometheus_base_url
        self.request_timeout = request_timeout

    async def run_query(self, query):
        """
        Execute a PromQL instant query using httpx asynchronously and return the result.
        :param query: PromQL query string.
        :return: JSON response from Prometheus.
        """
        async with httpx.AsyncClient(timeout=self.request_timeout) as client:
            response = await client.get(f"{self.prometheus_base_url}/api/v1/query", params={'query': query})
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

    async def run_range_query(self, query, start, end, step="1m"):
        """
        Execute a PromQL range query using httpx asynchronously and return the result.
        :param query: PromQL query string.
        :param start: Start time of the range in UNIX timestamp.
        :param end: End time of the range in UNIX timestamp.
        :param step: Resolution step width in duration format (e.g., '1m' for one minute).
        :return: JSON response from Prometheus.
        """
        params = {
            'query': query,
            'start': start.timestamp(),
            'end': end.timestamp(),
            'step': step
        }
        async with httpx.AsyncClient(timeout=self.request_timeout) as client:
            response = await client.get(f"{self.prometheus_base_url}/api/v1/query", params={'query': query},
                                        follow_redirects=False)
            if response.status_code == 302:
                print("Redirected to:", response.headers.get("Location"))
            elif response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")
# Example usage
async def main():
    executor = AsyncPrometheusQueryExecutor("https://prometheus.main2.staging.riskxint.com")
    query = "sum(kafka_log_log_size{ClusterName='dynamic'} > 0) by (topic)"
    # Define the time range as the past 24 hours
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)

    try:
        result = await executor.run_range_query(query, start_time, end_time)
        print(result)
    except Exception as e:
        print(f"Query failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())