# ActiveMQ Artemis Secuenciador Plugin

Plugin para ActiveMQ Artemis que reordena mensajes basándose en una propiedad de secuencia y los envía desde colas origen a colas destino configuradas.

## Descripción

Este plugin intercepta los mensajes de colas origen configuradas, los reordena según el valor de una propiedad personalizada (por defecto "idSecuencia"), y los envía a sus correspondientes colas destino. Es útil para garantizar que los mensajes sean procesados en el orden correcto cuando llegan desordenados.

El plugin soporta múltiples parejas de colas origen/destino, permitiendo configurar varios flujos de reordenación independientes en un mismo broker.

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

2. Configurar el plugin en el archivo `broker.xml` (ver ejemplos de configuración abajo).

3. Reiniciar el broker de ActiveMQ Artemis.

## Configuración

El plugin acepta las siguientes propiedades de configuración:

### Propiedades Generales

- **secuenciaProperty** (opcional): Nombre de la propiedad del mensaje que contiene el número de secuencia. Valor por defecto: `idSecuencia`
- **bufferSize** (opcional): Tamaño del buffer de mensajes para reordenamiento. Valor por defecto: `100`

### Configuración de Parejas de Colas

El plugin soporta dos formatos para configurar las parejas de colas origen/destino:

#### Formato 1: Propiedades Numeradas

Usar propiedades numeradas `colaOrigen.N` y `colaDestino.N` donde N es un índice numérico:

```xml
<broker-plugins>
  <broker-plugin class-name="es.enagas.artemis.plugin.SecuenciadorPlugin">
    <property key="secuenciaProperty" value="idSecuencia"/>
    <property key="bufferSize" value="100"/>
    <property key="colaOrigen.1" value="DLQ.Pedidos"/>
    <property key="colaDestino.1" value="Queue.PedidosOrdenados"/>
    <property key="colaOrigen.2" value="DLQ.Facturas"/>
    <property key="colaDestino.2" value="Queue.FacturasOrdenadas"/>
    <property key="colaOrigen.3" value="DLQ.Envios"/>
    <property key="colaDestino.3" value="Queue.EnviosOrdenados"/>
  </broker-plugin>
</broker-plugins>
```

#### Formato 2: String CSV

Usar una única propiedad `queuePairs` con formato CSV `origen:destino,origen:destino`:

```xml
<broker-plugins>
  <broker-plugin class-name="es.enagas.artemis.plugin.SecuenciadorPlugin">
    <property key="secuenciaProperty" value="idSecuencia"/>
    <property key="bufferSize" value="100"/>
    <property key="queuePairs" value="DLQ.Pedidos:Queue.PedidosOrdenados,DLQ.Facturas:Queue.FacturasOrdenadas,DLQ.Envios:Queue.EnviosOrdenados"/>
  </broker-plugin>
</broker-plugins>
```

**Nota:** Los dos formatos pueden combinarse. El plugin procesará todas las parejas configuradas en ambos formatos.

## Funcionamiento

Para cada pareja de colas configurada:

1. El plugin intercepta los mensajes de la cola origen antes de la entrega al consumidor
2. Los acumula en un buffer hasta alcanzar el tamaño configurado (`bufferSize`)
3. Ordena los mensajes por el valor de la propiedad de secuencia (`idSecuencia`)
4. Limpia el header `HDR_SCHEDULED_DELIVERY_TIME` si existe
5. Envía los mensajes ordenados atómicamente a la cola destino
6. Intenta reconocer (acknowledge) los mensajes originales para evitar reentrega

Cada pareja de colas tiene su propio buffer y se procesa de forma independiente.

**Nota Importante sobre Mensajes Origen**: El plugin intercepta mensajes antes de la entrega pero no impide completamente la entrega al consumidor de la cola origen. Los mensajes se copian y envían a la cola destino, y se intenta reconocer el mensaje original. Para un escenario de producción donde se requiere procesamiento exclusivo desde cola destino, considere:
- No tener consumidores activos en las colas origen
- Usar las colas origen solo como buffers de reordenamiento
- Los consumidores deberían conectarse a las colas destino

## Uso

Los mensajes deben incluir una propiedad con el número de secuencia. Ejemplo con un productor Java:

```java
Message message = session.createTextMessage("Contenido del mensaje");
message.setLongProperty("idSecuencia", 123);
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

- El plugin utiliza un buffer independiente para cada cola origen configurada
- Los mensajes sin la propiedad de secuencia se procesan al final (con secuencia null)
- Es importante ajustar el `bufferSize` según las necesidades de tu aplicación
- El buffer se procesa cuando alcanza el tamaño máximo o cuando se cierra el consumidor
- Los headers de schedule se limpian automáticamente antes de enviar a la cola destino
- Si no se configuran parejas de colas, el plugin no procesará ningún mensaje
