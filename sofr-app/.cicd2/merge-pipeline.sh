#!/bin/bash

if [[ $CI_MERGE_REQUEST_TITLE == "$(echo $CI_MERGE_REQUEST_TITLE | awk '/^\[AUTOMERGER\]/{print $0}')" ]]; 
then
  echo -e "MR автомёржера не проверяются на связи в Jira\e[0m"
  exit 0
fi

if [[ $CI_MERGE_REQUEST_TARGET_BRANCH_NAME == 'develop' || $CI_MERGE_REQUEST_TARGET_BRANCH_NAME == 'master' ]];
then
  echo -e "\e[1;33mMR в develop/master не проверяются на связи в Jira\e[0m"
  exit 0
fi

bash .cicd2/target-branch-control.sh
if [[ $? -eq 1 ]];
  then
    echo -e "\e[31mПроверка связей провалена\e[0m"
    exit 1
fi