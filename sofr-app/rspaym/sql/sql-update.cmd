@echo off
rem revision 1.1

echo SOFR SQL-update Liquibase simple automation script (c) RCHB-Intech 2022
echo:
echo Usage -- update-sql.cmd changelog.file
echo:


echo SQL-update: start checking parameters

echo SQL-update: checking enviroment variable %%SOFR_HOME%%
if not defined SOFR_HOME (
  echo SQL-update: %%SOFR_HOME%% variable not set, operations cancelled
  goto 1
)

echo SQL-update: checking %SOFR_HOME% catalog
if not exist %SOFR_HOME% (
  echo SQL-update: %SOFR_HOME% directory not found, operations cancelled
  goto 1
)

set schema=%1

echo SQL-update: checking %SOFR_HOME%\properties\%schema%\liquibase.properties
if not exist %SOFR_HOME%\properties\%schema%\liquibase.properties (
  echo SQL-update: %SOFR_HOME%\properties\%schema%\liquibase.properties not found, operations cancelled
  goto 1
)

set changelog=%2

echo SQL-update: checking changelog file
if not defined changelog (
  echo SQL-update: changelog not set, operations cancelled
  goto 1
)

if not exist %changelog% (
  echo SQL-update: changelog not found, operations cancelled
  goto 1
)

echo SQL-update: parameters verification successful
echo SQL-update: using properties %SOFR_HOME%\properties\%schema%\liquibase.properties
echo SQL-update: using changelog %changelog%
echo SQL-update: using changelog %schema%
echo SQL-update: now calling liquibase

call liquibase --defaultsFile=%SOFR_HOME%\properties\%schema%\liquibase.properties --changelog-file=%changelog% update

echo SQL-update: calling liquibase finished, please check liquibase logs
goto 2

:1
echo SQL-update: update failed

:2
