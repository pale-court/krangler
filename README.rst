Obtains, deduplicates and organizes Path of Exile data files from Steam.

Required environment variables:

* ``API_URL``: URL to changeapi service with which to register new appinfo
  files and find work from;
* ``APPINFO_DIR``: contains appinfo files from changemon to register with
  changeapi;
* ``ROOT_DIR``: root directory for the bulk data and index files as well as
  tool state;
* ``REMOTE_BUNDLE_DIR``: optional remote directory to grab existing bundles
  from;
* ``REMOTE_ZIP_DIR``: optional remote directory to grab existing data ZIPs
  from;
* ``STEAM_USER``: Steam user to download data files with;
* ``STEAM_PASSWORD``: password for Steam user;
* ``MOLLY_GUARD``: secret password for mutable changeapi access.

This tool preferably runs as a timer polling for new work and exiting when
nothing more is to be done.
