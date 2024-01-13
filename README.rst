Obtains, deduplicates and organizes Path of Exile data files from Steam.

Required environment variables:

* ``API_URL``: URL to changeapi service with which to register new appinfo
  files and find work from;
* ``APPINFO_DIR``: contains appinfo files from changemon to register with
  changeapi;
* ``ROOT_DIR``: root directory for the bulk data and index files as well as
  tool state;
* ``DEPOT_DOWNLOADER``: optional path to the DepotDownloader executable to
  download fresh data from Steam, not needed if only using existing data;
* ``CACHED_BUNDLE_DIR``: optional directory to grab existing bundles
  from;
* ``CACHED_PACK_DIR``: optional directory to grab existing GGPK packs
  from;
* ``CACHED_ZIP_DIR``: optional directory to grab existing data ZIPs
  from;
* ``ALLOW_DOWNLOADS``: indicates if new data can be downloaded on demand;
* ``ALLOW_ZIPPING``: indicates if new data should be archived for eventual reprocessing;
* ``STEAM_USER``: Steam user to download data files with;
* ``STEAM_PASSWORD``: password for Steam user;
* ``STEAM_ID_BASE``: base offset for Steam login IDs to allow concurrent logins;
* ``MOLLY_GUARD``: secret password for mutable changeapi access.


This tool preferably runs as a timer polling for new work and exiting when
nothing more is to be done.
