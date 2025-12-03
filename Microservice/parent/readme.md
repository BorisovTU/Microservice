✅ Что делает этот POM:
Управляет версиями зависимостей (через dependencyManagement).
Определяет версии Spring Boot, Spring Cloud, Spring Authorization Server.
Включает lombok, mapstruct, postgresql, mongodb, redis.
Управляет плагинами (например, maven-compiler-plugin с поддержкой lombok и mapstruct).
Устанавливает Java 17, UTF-8 кодировку.
📌 Как использовать:
Создайте модуль borisov-parent в вашем Maven-проекте.
Добавьте в settings.xml или в репозиторий (Nexus, JitPack, GitHub Packages).
В других модулях используйте:

````
<parent>
    <groupId>ru.borisov.group</groupId>
    <artifactId>borisov-parent</artifactId>
    <version>1.0.0</version>
</parent>
````      

Или как BOM:

````
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>ru.borisov.group</groupId>
            <artifactId>borisov-parent</artifactId>
            <version>1.0.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
````