# Cloud-sync

This is a script that allows the syncronisation of Rackspace Cloudfiles (Or any SWIFT object store)
with a local directory. Currently it allows the user to pipe a list of files into it that are to be
uploaded to cloud files.


Features
========
* Lightweight - Currently the only non standard python library required is python-cloudfiles.
    The aim is NOT to require anything else, and hence ensuring maximum portability
* Support for Cloudfiles UK and US along with any SWIFT object store
* Comparision of files to ensure Bandwidth is not wasted by comparing the remote & local MD5 and Modified Time

See the projects github wiki page for more info
https://github.com/welbymcroberts/cloudfiles-sync/wiki

Example usage
=============

Authors
===============
* Welby McRoberts <cloudfilessync+welby@whmcr.com> Main developer.
* Darren Birket - Contributor, Tester and provider of suggestions.

Change Log
============

1.9.1
* A rewrite of cloudfiles-sync to support multiple cloud providers, and code cleanup
* Inital thoughts of threading

1.0.0
* First version