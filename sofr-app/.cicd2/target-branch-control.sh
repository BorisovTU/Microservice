#!/bin/bash
TXT_RED="\e[31m"
TXT_YEL="\e[1;33m"
TXT_CLEAR="\e[0m"

function checkMRforStory {
  release_task=$(echo "$jira_body" | jq  '.fields.issuelinks[] | select(.type.name=="Включено в Релиз") | select(.type.outward=="Входит в ")' | jq  '.outwardIssue.key | select(contains("CCBO"))')
  echo "Release for story is: "
  echo ${release_task}
  if [[ -z "$release_task" ]]; then
    fix_version=$(echo "$jira_body" | jq  '.fields.fixVersions[].name')
    echo "Story is not linked with release task, fixVersion is ${fix_version}. Please address release manager."
    exit 1
  fi
  echo "Story is linked with release, check that release is correct."
  release_task_for_url=$(echo "$release_task" | tr -d '"')
  http_response=$(curl --silent --show-error --write-out "%{http_code}" -o response.txt \
  --user "$SOFR_APP_JIRA_USERNAME":"$SOFR_APP_JIRA_PASSWORD" \
  --header 'Content-Type: application/json' \
  --request GET "https://sdlc.go.rshbank.ru/jira/rest/api/2/issue/${release_task_for_url}")
  release_number=$(cat response.txt | jq  '.fields.summary' | tr -d '"' | awk -F '.' '{if ($5 == "") $5 = ".0";  str = $4$5; sub(/MR/, ".", str); print(str)}')
  target_version=$(echo $CI_MERGE_REQUEST_TARGET_BRANCH_NAME | awk -F '-' '{print $2}')
  echo "Release number is: "
  echo ${release_number}
  if [[ ! ($target_version < ${release_number}) ]]; then
    echo "Release number: ${release_number}"
    echo "Target version: $target_version"
    echo "Target branch is correct"
    exit 0
  else
    echo "Release number: ${release_number}"
    echo "Target version: $target_version"
    echo "Target branch is incorrect!"
    exit 1
  fi
}

function checkMRforDefect {
  echo "$jira_body" | jq --exit-status '.fields.issuelinks[]' | jq --exit-status 'select(.type.inward | contains("Включает в себя"))' | jq --exit-status '.inwardIssue.fields.issuetype.name' |  tr -d '"' > arrayfile
  echo "$jira_body" | jq --exit-status '.fields.issuelinks[]' | jq --exit-status 'select(.type.inward | contains("Включает в себя"))' | jq --exit-status '.inwardIssue.key' |  tr -d '"' > keyfile
  readarray -t typelinks < arrayfile
  readarray -t keylinks < keyfile
  if [[ ${#typelinks[@]} -eq 0 ]]; then
    echo "No include in stories for defect?"
    exit 1
  fi
  echo "Linked issues are (typelinks): "
  echo "${typelinks[@]}"
  echo "Linked issues are (keylinks): "
  echo "${keylinks[@]}"
  story_url=$(echo "$jira_body" | jq --exit-status '.fields.issuelinks[]' | jq --exit-status 'select(.type.inward | contains("Включает в себя"))' | jq --exit-status '.inwardIssue.self' |  tr -d '"')
  # get story data from jira
  jira_body=$(curl --fail --silent --show-error --user "${SOFR_APP_JIRA_USERNAME}:${SOFR_APP_JIRA_PASSWORD}" "$story_url")
  if [[ $jira_body == '[]' ]]; then echo "Jira API response body is empty"; exit 1; fi
  # determine release type issue (rti) index
  echo "$jira_body" | jq --exit-status '.fields.issuelinks[].outwardIssue.fields.issuetype.name' | tr -d '"' > arrayfile
  readarray -t typelinks < arrayfile
  echo "Linked issues are (arrayfile): "
  cat arrayfile
  echo "Linked issues are (typelinks): "
  echo "${typelinks[@]}"
  rti_indexes=()
  i=0
  while [[ $i -lt ${#typelinks[@]} ]]
  do
    if [[ ${typelinks[$i]} == 'Релиз' ]]; then rti_indexes+=($i); fi
    ((i++))
  done

  if [ ${#rti_indexes[@]} -eq 0 ]; then
    echo "No releases for story task, exiting with error!"
    exit 1
  fi

  # read release issue headers
  releases=()
  for j in $rti_indexes
  do
    releases+=($(echo "$jira_body" | jq --exit-status ".fields.issuelinks[$j].outwardIssue.fields.summary" | tr -d '"' | awk -F '.' '{if ($5 == "") $5 = ".0";  str = $4$5; sub(/MR/, ".", str); print(str)}'))
  done
  # determine target release version
  target_version=$(echo $CI_MERGE_REQUEST_TARGET_BRANCH_NAME | awk -F '-' '{print $2}')
  # check is there is equal
  for ((j=0; j<${#releases[@]}; j++))
  do
    if [[ $target_version == ${releases[$j]} ]]
    then
      echo "Issue version: ${releases[$j]}"
      echo "Target version: $target_version"
      echo "Target branch is correct"
      exit 0
    fi
  done
  # make another check if there is no equal elements
  # sort releases array
  IFS=$'\n'; releases=($(sort -r <<< "${releases[*]}")); unset IFS
  # set issue release max version
  issue_version=${releases[0]}
  # compare versions
  echo "Issue version: $issue_version"
  echo "Target version: $target_version"
  if [[ $issue_version < $target_version ]];
    then
      echo "Issue version before target branch!"
      if [[ $issue_version == ".0" ]];
      then echo "Issue version is inorrect"; exit 1; fi;
    echo "Target branch is correct"; exit 0
    else echo "Target branch is incorrect"; exit 1;
  fi
}

# get related issue from gitlab
gitlab_body=$(curl --fail --silent --show-error --header "PRIVATE-TOKEN:$SOFR_APP_READ_API_PERSONAL_TOKEN" "$CI_API_V4_URL/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID")
if [[ $gitlab_body == '[]' ]]; then echo "Gitlab API response body is empty"; exit 1; fi
# check if MR in draft state or not
draft_state=$(echo "$gitlab_body" | jq  '.draft')
author=$(echo "$gitlab_body" | jq  '.author.username')
if [[ $draft_state == *true* ]]; then echo "Merge request is in draft state, exiting"; exit 0; fi
if [[ $author == *"abramova-irivl"* ]]; then echo "Merge request for user documentation, exiting"; exit 0; fi

related_issue="$(echo "$gitlab_body" | jq --exit-status '.description' | tr -d '"\t\r\n' | awk '{print $NF""}' | awk -F 'https' '{print $1}' | tr -d '[](')"
echo "related issue is: "
echo $related_issue
if [[ -z ${related_issue} ]]; then
  echo "No related issue for MR, please specify related issue!";
  exit 1
fi
# get related issue data from jira
jira_body=$(curl --fail --silent --show-error --user "$SOFR_APP_JIRA_USERNAME:$SOFR_APP_JIRA_PASSWORD" "https://sdlc.go.rshbank.ru/jira/rest/api/latest/issue/$related_issue")
if [[ $jira_body == '[]' ]]; then echo "Jira API response body is empty"; exit 1; fi
issue_type=$(echo "$jira_body" | jq --exit-status '.fields.issuetype.name' | tr -d '"')
echo "Issue type:"
echo ${issue_type}
# Проверяем статус тикета
if [[ $issue_type == "Дефект" || $issue_type == "Задача" || $issue_type == "Подзадача" ]]
  then
    issue_status=$(echo "$jira_body" | jq --exit-status '.fields.status.name' | tr -d '"')
    if [[ $issue_status == "Закрыт" || $issue_status == "Протестирован успешно" || $issue_status == "Готова в ПРОМ" || $issue_status == "Закрыта" || $issue_status == "Тестирование завершено" || $issue_status == "Внедрено" ]]
      then
        echo -e "${TXT_YEL}Связанный тикет для данной доработки находится в статусе: ${TXT_CLEAR}$issue_status${TXT_YEL}. Мёрж ${TXT_RED}запрещён${TXT_YEL}."
        exit 1
    fi
fi
###############
if [[ ${issue_type} == *История* ]]; then
  echo "MR created for story";
  checkMRforStory
elif [[ ${issue_type} == *Подзадача* ]]; then
  echo "Subtask found, attempt to find parent story"
  parent_task=$(echo "$jira_body" | jq  '.fields.parent.key' | tr -d '"')
  echo "Parent story for subtask is: "
  echo ${parent_task}
  if [[ -z ${parent_task} ]]; then
    echo "No parent task for subtask found, exiting!";
  exit 1
  fi
  jira_body=$(curl --fail --silent --show-error --user "$SOFR_APP_JIRA_USERNAME:$SOFR_APP_JIRA_PASSWORD" "https://sdlc.go.rshbank.ru/jira/rest/api/latest/issue/$parent_task")
  if [[ $jira_body == '[]' ]]; then echo "Jira API response body for parent task is empty"; exit 1; fi
  checkMRforStory
fi

# check is history type issue (hti) is only one
if [[ $(echo "$jira_body" | jq --exit-status '[.fields.issuelinks[].inwardIssue.fields.issuetype | select(.name=="История")] | length') -eq 0 ]]; then
  echo "No history links for defect, exiting with error!";
  exit 1;
  fi
if [[ $(echo "$jira_body" | jq --exit-status '[.fields.issuelinks[].inwardIssue.fields.issuetype | select(.name=="История")] | length') -gt 3 ]]; then
  echo "There are more than three history links, exiting with error";
  exit 1;
  fi
if [[ $(echo "$jira_body" | jq --exit-status '[.fields.issuelinks[].inwardIssue.fields.issuetype | select(.name=="История")] | length') -gt 1 ]]; then
  summaries=$(echo "$jira_body" | jq  '.fields.issuelinks[].inwardIssue.fields | select(.issuetype.name=="История")'| jq  '.summary')
  echo "Summary for linked stories are: "
  echo ${summaries}
  echo "Keys for linked stories are: "
  echo ${keys}
  sl3_flag=0;
  nt_flag=0;
  if [[ ${summaries} == *"СОФР. 3 линия"* ]]; then
    ((sl3_flag++))
    fi
  if [[ ${summaries} == *"НТ"* ]]; then
    ((nt_flag++))
    fi
  echo "SL3 flag: "
  echo ${sl3_flag}
  echo "NT flag: "
  echo ${nt_flag}
  if [[ ${sl3_flag} == 1 ]]; then
    echo "SL3 defect, continue with target branch check"
    checkMRforDefect
  elif [[ ${nt_flag} == 1 ]]; then
    echo "Defect linked with load testing, continue with target branch check!"
    checkMRforDefect
  else
    echo "There are more than one history links and it is not SL3 defect, and not load test defect, exiting with error.";
    exit 1;
  fi
fi
if [[ $(echo "$jira_body" | jq --exit-status '[.fields.issuelinks[].inwardIssue.fields.issuetype | select(.name=="История")] | length') -eq 1 ]]; then
  echo "Defect linked with 1 story, continue with target branch check."
  checkMRforDefect
fi