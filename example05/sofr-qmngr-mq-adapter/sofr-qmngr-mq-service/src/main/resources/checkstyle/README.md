## Инструкция по настройке плагина CheckStyle в IntelliJIdea
***
После установки CheckStyle плагина, необходимо указать путь до файла checkstyle.xml:
- File → Settings → Tools → CheckStyle
- Click `+` in `Configuration File` tab
- Specify `checkstyle.xml` file path

После того, как путь будет указан, необходимо ввести путь до `suppressions.xml` файла - `checkstyle.config.path`.
Этот путь соответствует пути на файловой системе до папки /src/main/resources/checkstyle/.
<br/>По завершении настройки можно запустить проверку проекта через интерфейс IntelliJIdea.

### Настройка автоформатирования
Для настройки автоформатирования нужно указать путь к файлу checkstyle.xml следующим образом (IntelliJIdea):

- File → Settings → Editor → Code Style.
- Click the small gear icon next to “Scheme”, Select “Import Scheme” → CheckStyle Configuration.
- Select our checkstyle.xml.
- Click OK.
- Click Apply.

### Запуск проверки

Запустить проверку можно либо через интерфейс плагина в IntelliJIdea, либо командой:

    mvn validate

#### Информацию о плагине и его конфигурации можно найти [здесь](https://checkstyle.sourceforge.io/config.html).
