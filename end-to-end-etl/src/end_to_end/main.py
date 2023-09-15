import json
from load import Loader, copy_object_between_buckets
from extraction import Extractor
from process import process_vehicles, process_operating_periods, process_events
from end_to_end.config import (
    setup_logger,
    EXTRACT_S3_BUCKET,
    LOAD_S3_BUCKET,
    LAST_PROCESSED_FILE_NAME,
    HOST,
    USER,
    PASSWD,
    DB,
)

# Set up logger
logger = setup_logger(__name__, "ete.log")


def process_files(loader, extractor, unprocessed_files):
    for file, file_obj in unprocessed_files:
        logger.debug(f"Processing file: {file['Key']}")
        for record in file_obj["Body"].iter_lines():
            json_record = json.loads(record)

            # Vehicle Processing
            if json_record["on"] == "vehicle" and json_record["event"] not in [
                "register",
                "deregister",
            ]:
                logger.debug(json_record)
                vehicles_data = process_vehicles(json_record)
                loader.insert_data(
                    table_name="vehicles",
                    columns="id, lat, lng, at",
                    data=vehicles_data,
                )
                logger.debug("Vehicle data inserted.")

            # Operating Period Processing
            elif json_record["on"] == "operating_period":
                print(json_record)
                operating_period_data = process_operating_periods(json_record)
                loader.insert_data(
                    table_name="operating_periods",
                    columns="id, start, finish",
                    data=operating_period_data,
                )
                logger.debug("Operating period data inserted.")

            # Event Processing
            if json_record["event"] != "update":
                events_data = process_events(json_record)
                loader.insert_data(
                    table_name="events",
                    columns="""event_type,
                        entity_type,
                        at,
                        organization_id,
                        vehicle_id,
                        operating_period_id
                    """,
                    data=events_data,
                )
                logger.debug("Event data inserted.")

        copy_object_between_buckets(
            extract=EXTRACT_S3_BUCKET, load=LOAD_S3_BUCKET, file_name=file["Key"]
        )

        extractor.set_last_processed_file_name(
            filename=LAST_PROCESSED_FILE_NAME,
            bucket=LOAD_S3_BUCKET,
            start_after=file["Key"],
        )

        logger.info(f"Finished processing file: {file['Key']}")


def main():
    try:
        logger.info("Starting main execution.")
        loader = Loader(host=HOST, user=USER, passwd=PASSWD, db=DB)
        extractor = Extractor()

        last_processed_file_name = extractor.get_last_processed_file_name(
            LOAD_S3_BUCKET, LAST_PROCESSED_FILE_NAME
        )

        unprocessed_files = extractor.get_unprocessed_files(
            EXTRACT_S3_BUCKET, last_processed_file_name
        )

        process_files(loader, extractor, unprocessed_files)

    finally:
        if loader.connection.is_connected():
            loader.connection.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
