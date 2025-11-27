TXT_RED="\e[31m"
TXT_YEL="\e[1;33m"
TXT_CLEAR="\e[0m"
# Проверяем одобрение MR
approvers=$(curl --fail --silent --show-error --header "PRIVATE-TOKEN:$SOFR_APP_READ_API_PERSONAL_TOKEN" "$CI_API_V4_URL/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/approvals" | jq --exit-status '.approved_by')
if [[ $approvers == '[]' ]];
  then 
      echo -e "${TXT_YEL}Нет одобрений${TXT_CLEAR}"
      echo 'CHECK_APPROVE=missing' > $CI_PROJECT_DIR/is_approved.txt
  else # Уполномоченные сотрудники: Бутузов, Пономарев, Суриков, Жилин, Сидорович, Минаева, Шентеряков
    approver_ids=$(echo $approvers | jq --exit-status '.[].user.id')
    k=0
    for id in $approver_ids; 
      do
        if [[ $id -eq 13799 || $id -eq 12420 || $id -eq 955 || $id -eq 13797 || $id -eq 13383 || $id -eq 9965 || $id -eq 1965 ]];
          then
          approver_name=$(echo $approvers | jq --exit-status ".[$k].user.username")
          echo -e "${TXT_YEL}MR одобрен пользователем $approver_name${TXT_CLEAR}"
          echo -e "${TXT_YEL}Джоб пропущен${TXT_CLEAR}"
          echo 'CHECK_APPROVE=pass' > $CI_PROJECT_DIR/is_approved.txt
          exit 0
        fi
        ((k++))
      done
fi
echo 'CHECK_APPROVE=missing' > $CI_PROJECT_DIR/is_approved.txt
