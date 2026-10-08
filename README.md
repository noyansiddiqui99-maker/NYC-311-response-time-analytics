# Pakistan-s-Digital-Payment-Analytics

Each page of data is saved as a gzipped JSON file with one record per line. Spark reads this format directly and unzips it on its own, so no extra code is needed. Keeping one file per page means we can reload any single batch whenever we want.
