# Pakistan-s-Digital-Payment-Analytics

Each page of data is saved as a gzipped JSON file with one record per line. Spark reads this format directly and unzips it on its own, so no extra code is needed. Keeping one file per page means we can reload any single batch whenever we want.


Bronze Layer — Raw volume to Bronze
Loads every .json.gz file in the raw volume into bronze_nyc311_new. Each file becomes its own batch, with the batch_id taken from the filename, so page_3.json.gz becomes batch_id = page_3.

How to run it

batch_id widget	What happens
blank (default)	Loads every file in the folder, one batch per file
page_7	Loads only that file — use this to repair or backfill one batch
Idempotency. Each batch is written with replaceWhere batch_id = '<batch>', so re-running replaces only that batch's rows and leaves the others untouched. Running the notebook twice gives exactly the same table as running it once.

Bronze keeps everything. Duplicate unique_key values are not removed here. If the same service request appears in three files, Bronze stores all three versions, because Bronze is the record of what actually arrived. Silver picks the most recent version.

Bronze data dictionary

print("| Column | Type | Nullable | Key | Description |")
print("|---|---|---|---|---|")
for r in dictionary.collect():
    print(f"| {r.column_name} | {r.data_type} | {r.nullable} | "
          f"{r.key} | {r.description} |")
