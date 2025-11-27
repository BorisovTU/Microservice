@echo off

call liquibase --defaultsFile=liquibase.properties --changelog-file=main-changelog.xml --log-level=info --log-file=liquibase.log update

