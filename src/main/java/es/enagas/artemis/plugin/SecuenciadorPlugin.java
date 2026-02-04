package es.enagas.artemis.plugin;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Plugin de ActiveMQ Artemis que reordena mensajes basándose en una propiedad de secuencia.
 * 
 * Este plugin intercepta los mensajes antes de ser entregados al consumidor y los reordena
 * según el valor de una propiedad personalizada (por defecto "idSecuencia").
 * Soporta múltiples parejas de colas origen/destino.
 * 
 * Configuración en broker.xml:
 * <broker-plugins>
 *   <broker-plugin class-name="es.enagas.artemis.plugin.SecuenciadorPlugin">
 *     <property key="secuenciaProperty" value="idSecuencia"/>
 *     <property key="bufferSize" value="100"/>
 *     <property key="colaOrigen.1" value="queue.origen1"/>
 *     <property key="colaDestino.1" value="queue.destino1"/>
 *     <property key="colaOrigen.2" value="queue.origen2"/>
 *     <property key="colaDestino.2" value="queue.destino2"/>
 *   </broker-plugin>
 * </broker-plugins>
 * 
 * O usando formato CSV:
 * <broker-plugins>
 *   <broker-plugin class-name="es.enagas.artemis.plugin.SecuenciadorPlugin">
 *     <property key="queuePairs" value="queue.origen1:queue.destino1,queue.origen2:queue.destino2"/>
 *   </broker-plugin>
 * </broker-plugins>
 */
public class SecuenciadorPlugin implements ActiveMQServerPlugin {

    private static final Logger logger = Logger.getLogger(SecuenciadorPlugin.class);
    
    // Clase interna para representar una pareja de colas
    private static class QueuePair {
        final String origen;
        final String destino;
        
        QueuePair(String origen, String destino) {
            this.origen = origen;
            this.destino = destino;
        }
        
        @Override
        public String toString() {
            return "QueuePair{origen='" + origen + "', destino='" + destino + "'}";
        }
    }
    
    // Propiedad del mensaje que contiene el número de secuencia
    private String secuenciaProperty = "idSecuencia";
    
    // Tamaño del buffer de mensajes para reordenamiento
    private int bufferSize = 100;
    
    // Lista de parejas de colas configuradas
    private final List<QueuePair> queuePairs = new ArrayList<>();
    
    // Mapa de cola origen a cola destino
    private final Map<String, String> origenToDestino = new ConcurrentHashMap<>();
    
    // Buffer de mensajes por cola origen
    private final Map<String, List<MessageReference>> messageBuffers = new ConcurrentHashMap<>();
    
    // Referencia al servidor para enviar mensajes
    private ActiveMQServer server;
    
    @Override
    public void init(Map<String, String> properties) {
        logger.info("Inicializando SecuenciadorPlugin");
        
        if (properties != null) {
            // Configurar propiedad de secuencia
            if (properties.containsKey("secuenciaProperty")) {
                secuenciaProperty = properties.get("secuenciaProperty");
                logger.info("Propiedad de secuencia configurada: " + secuenciaProperty);
            }
            
            // Configurar tamaño de buffer
            if (properties.containsKey("bufferSize")) {
                try {
                    bufferSize = Integer.parseInt(properties.get("bufferSize"));
                    logger.info("Tamaño de buffer configurado: " + bufferSize);
                } catch (NumberFormatException e) {
                    logger.warn("Valor inválido para bufferSize, usando valor por defecto: " + bufferSize);
                }
            }
            
            // Parsear parejas de colas en formato CSV
            if (properties.containsKey("queuePairs")) {
                parseQueuePairsCSV(properties.get("queuePairs"));
            }
            
            // Parsear parejas de colas en formato numerado (colaOrigen.N, colaDestino.N)
            parseNumberedQueuePairs(properties);
        }
        
        // Construir mapa de origen a destino
        for (QueuePair pair : queuePairs) {
            origenToDestino.put(pair.origen, pair.destino);
            logger.info("Configurada pareja de colas: " + pair);
        }
        
        if (queuePairs.isEmpty()) {
            logger.warn("No se configuraron parejas de colas. El plugin no procesará ningún mensaje.");
        }
        
        logger.info("SecuenciadorPlugin inicializado correctamente con " + queuePairs.size() + " pareja(s) de colas");
    }
    
    /**
     * Parsea parejas de colas en formato CSV: "origen1:destino1,origen2:destino2"
     */
    private void parseQueuePairsCSV(String csv) {
        if (csv == null || csv.trim().isEmpty()) {
            return;
        }
        
        String[] pairs = csv.split(",");
        for (String pair : pairs) {
            String[] parts = pair.trim().split(":");
            if (parts.length == 2) {
                String origen = parts[0].trim();
                String destino = parts[1].trim();
                if (!origen.isEmpty() && !destino.isEmpty()) {
                    queuePairs.add(new QueuePair(origen, destino));
                }
            } else {
                logger.warn("Formato de pareja inválido, se esperaba 'origen:destino': " + pair);
            }
        }
    }
    
    /**
     * Parsea parejas de colas en formato numerado: colaOrigen.N, colaDestino.N
     */
    private void parseNumberedQueuePairs(Map<String, String> properties) {
        Map<Integer, String> origenes = new HashMap<>();
        Map<Integer, String> destinos = new HashMap<>();
        
        // Buscar todas las propiedades con formato colaOrigen.N y colaDestino.N
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            
            if (key.startsWith("colaOrigen.")) {
                try {
                    int index = Integer.parseInt(key.substring("colaOrigen.".length()));
                    origenes.put(index, value);
                } catch (NumberFormatException e) {
                    logger.warn("Formato de índice inválido en propiedad: " + key);
                }
            } else if (key.startsWith("colaDestino.")) {
                try {
                    int index = Integer.parseInt(key.substring("colaDestino.".length()));
                    destinos.put(index, value);
                } catch (NumberFormatException e) {
                    logger.warn("Formato de índice inválido en propiedad: " + key);
                }
            }
        }
        
        // Emparejar origenes con destinos por índice
        for (Map.Entry<Integer, String> origenEntry : origenes.entrySet()) {
            Integer index = origenEntry.getKey();
            String origen = origenEntry.getValue();
            String destino = destinos.get(index);
            
            if (destino != null && !origen.trim().isEmpty() && !destino.trim().isEmpty()) {
                queuePairs.add(new QueuePair(origen.trim(), destino.trim()));
            } else {
                logger.warn("No se encontró colaDestino." + index + " para colaOrigen." + index);
            }
        }
        
        // Advertir sobre destinos sin origen
        for (Map.Entry<Integer, String> destinoEntry : destinos.entrySet()) {
            Integer index = destinoEntry.getKey();
            if (!origenes.containsKey(index)) {
                logger.warn("Se encontró colaDestino." + index + " sin colaOrigen." + index + " correspondiente");
            }
        }
    }

    @Override
    public void registered(ActiveMQServer server) {
        logger.info("SecuenciadorPlugin registrado en el servidor: " + server.getIdentity());
        this.server = server;
    }

    @Override
    public void unregistered(ActiveMQServer server) {
        logger.info("SecuenciadorPlugin desregistrado del servidor: " + server.getIdentity());
        messageBuffers.clear();
    }

    /**
     * Intercepta los mensajes antes de ser entregados al consumidor.
     * Solo procesa mensajes de colas origen configuradas.
     * Reordena los mensajes según su número de secuencia y los envía a la cola destino.
     */
    @Override
    public void beforeDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {
        Queue queue = reference.getQueue();
        String queueName = queue.getName().toString();
        
        // Verificar si esta cola está configurada como cola origen
        if (!origenToDestino.containsKey(queueName)) {
            // No es una cola origen configurada, no hacer nada
            return;
        }
        
        // Obtener el buffer de mensajes para esta cola origen
        List<MessageReference> buffer = messageBuffers.computeIfAbsent(
            queueName, 
            k -> new ArrayList<>()
        );
        
        // Agregar el mensaje al buffer
        synchronized (buffer) {
            buffer.add(reference);
            
            // Si el buffer alcanza el tamaño máximo, procesarlo
            if (buffer.size() >= bufferSize) {
                procesarBuffer(queueName, buffer);
            }
        }
    }

    /**
     * Procesa y reordena los mensajes en el buffer según su secuencia.
     * Luego envía los mensajes ordenados a la cola destino correspondiente.
     */
    private void procesarBuffer(String queueOrigen, List<MessageReference> buffer) {
        if (buffer.isEmpty()) {
            return;
        }
        
        String queueDestino = origenToDestino.get(queueOrigen);
        if (queueDestino == null) {
            logger.warn("No se encontró cola destino para cola origen: " + queueOrigen);
            buffer.clear();
            return;
        }
        
        try {
            // Ordenar mensajes por número de secuencia
            buffer.sort(new Comparator<MessageReference>() {
                @Override
                public int compare(MessageReference ref1, MessageReference ref2) {
                    Long seq1 = obtenerSecuencia(ref1);
                    Long seq2 = obtenerSecuencia(ref2);
                    
                    if (seq1 == null && seq2 == null) return 0;
                    if (seq1 == null) return 1;
                    if (seq2 == null) return -1;
                    
                    return seq1.compareTo(seq2);
                }
            });
            
            logger.debug("Buffer procesado y ordenado con " + buffer.size() + " mensajes para " + queueOrigen + " -> " + queueDestino);
            
            // Enviar mensajes ordenados a la cola destino
            enviarMensajesACola(queueDestino, buffer);
            
        } catch (Exception e) {
            logger.error("Error al procesar buffer de mensajes para " + queueOrigen, e);
        } finally {
            buffer.clear();
        }
    }
    
    /**
     * Envía los mensajes ordenados a la cola destino.
     * Limpia el header de schedule y realiza envío atómico.
     */
    private void enviarMensajesACola(String queueDestino, List<MessageReference> mensajes) {
        if (server == null) {
            logger.error("Servidor no disponible para enviar mensajes");
            return;
        }
        
        try {
            Queue destinoQueue = server.locateQueue(SimpleString.of(queueDestino));
            if (destinoQueue == null) {
                logger.warn("Cola destino no encontrada: " + queueDestino);
                return;
            }
            
            for (MessageReference ref : mensajes) {
                try {
                    Message mensaje = ref.getMessage().copy();
                    
                    // Limpiar header de schedule si existe
                    if (mensaje instanceof ICoreMessage) {
                        ICoreMessage coreMsg = (ICoreMessage) mensaje;
                        coreMsg.removeProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
                    }
                    
                    // Enviar mensaje a la cola destino
                    destinoQueue.route(mensaje, null);
                    
                } catch (Exception e) {
                    logger.error("Error al enviar mensaje individual a " + queueDestino, e);
                }
            }
            
            logger.info("Enviados " + mensajes.size() + " mensajes ordenados a " + queueDestino);
            
        } catch (Exception e) {
            logger.error("Error al enviar mensajes a cola destino: " + queueDestino, e);
        }
    }

    /**
     * Obtiene el número de secuencia de un mensaje.
     */
    private Long obtenerSecuencia(MessageReference reference) {
        try {
            Message message = reference.getMessage();
            if (message instanceof ICoreMessage) {
                ICoreMessage coreMessage = (ICoreMessage) message;
                Object secuencia = coreMessage.getObjectProperty(secuenciaProperty);
                
                if (secuencia instanceof Number) {
                    return ((Number) secuencia).longValue();
                } else if (secuencia instanceof String) {
                    return Long.parseLong((String) secuencia);
                }
            }
        } catch (Exception e) {
            logger.warn("Error al obtener secuencia del mensaje: " + e.getMessage());
        }
        return null;
    }

    /**
     * Limpia el buffer cuando se cierra un consumidor.
     */
    @Override
    public void afterCloseConsumer(ServerConsumer consumer, boolean failed) throws ActiveMQException {
        Queue queue = consumer.getQueue();
        if (queue != null) {
            String queueName = queue.getName().toString();
            
            // Solo procesar si es una cola origen configurada
            if (origenToDestino.containsKey(queueName)) {
                List<MessageReference> buffer = messageBuffers.get(queueName);
                
                if (buffer != null) {
                    synchronized (buffer) {
                        procesarBuffer(queueName, buffer);
                    }
                }
            }
        }
    }
}
