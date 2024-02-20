import prefect
from prefect import flow, get_run_logger
from platform import node, platform, python_version



@flow
def maintenance():
    version = prefect.__version__
    logger = get_run_logger()
    logger.info("Network: %s. Instance: %s. Agent is healthy ✅️", node(), platform())
    logger.info(f"Python = {python_version}. Prefect = {version} 🚀")


if __name__ == "__main__":
    maintenance()
