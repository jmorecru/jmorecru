# ActiveMQ Artemis Secuenciador Plugin

Plugin para ActiveMQ Artemis que reordena mensajes basándose en una propiedad de secuencia.

## Descripción

Este plugin intercepta los mensajes antes de ser entregados al consumidor y los reordena según el valor de una propiedad personalizada (por defecto "secuencia"). Es útil para garantizar que los mensajes sean procesados en el orden correcto cuando llegan desordenados.

## Construcción

Para compilar el plugin y generar el JAR:

```bash
mvn clean package
```

El JAR generado se encontrará en `target/artemis-secuenciador-plugin-1.0.0.jar`

## Instalación

1. Copiar el JAR generado a la carpeta `lib` del broker de ActiveMQ Artemis:
   ```bash
   cp target/artemis-secuenciador-plugin-1.0.0.jar $ARTEMIS_HOME/lib/
   ```

2. Configurar el plugin en el archivo `broker.xml`:
   ```xml
   <broker-plugins>
     <broker-plugin class-name="es.enagas.artemis.plugin.SecuenciadorPlugin">
       <property key="secuenciaProperty" value="secuencia"/>
       <property key="bufferSize" value="100"/>
     </broker-plugin>
   </broker-plugins>
   ```

3. Reiniciar el broker de ActiveMQ Artemis.

## Configuración

El plugin acepta las siguientes propiedades de configuración:

- **secuenciaProperty** (opcional): Nombre de la propiedad del mensaje que contiene el número de secuencia. Valor por defecto: `secuencia`
- **bufferSize** (opcional): Tamaño del buffer de mensajes para reordenamiento. Valor por defecto: `100`

## Uso

Los mensajes deben incluir una propiedad con el número de secuencia. Ejemplo con un productor Java:

```java
Message message = session.createTextMessage("Contenido del mensaje");
message.setLongProperty("secuencia", 123);
producer.send(message);
```

## Requisitos

- Java 11 o superior
- ActiveMQ Artemis 2.44.0
- Maven 3.6 o superior

## Versión

- **Versión del plugin**: 1.0.0
- **Versión de ActiveMQ Artemis**: 2.44.0

## Estructura del Proyecto

```
.
├── pom.xml                                           # Configuración Maven
└── src
    └── main
        └── java
            └── es
                └── enagas
                    └── artemis
                        └── plugin
                            └── SecuenciadorPlugin.java  # Implementación del plugin
```

## Notas

- El plugin utiliza un buffer para acumular mensajes antes de reordenarlos
- Los mensajes sin la propiedad de secuencia se procesan al final
- Es importante ajustar el `bufferSize` según las necesidades de tu aplicación
