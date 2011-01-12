# Cloudfiles-sync

This is a script that allows the user to pipe a list of files into in that are to be uploaded to cloud files. The file modified time is compared if the file already exists, if this is the case it will not re-upload the file.

# Example usage
`cd /backups && find -type f | python cfsync.py`
