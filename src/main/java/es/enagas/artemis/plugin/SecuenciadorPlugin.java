package es.enagas.artemis.plugin;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Plugin de ActiveMQ Artemis que reordena mensajes basándose en una propiedad de secuencia.
 * 
 * Este plugin intercepta los mensajes antes de ser entregados al consumidor y los reordena
 * según el valor de una propiedad personalizada (por defecto "secuencia").
 * 
 * Configuración en broker.xml:
 * <broker-plugins>
 *   <broker-plugin class-name="es.enagas.artemis.plugin.SecuenciadorPlugin">
 *     <property key="secuenciaProperty" value="secuencia"/>
 *     <property key="bufferSize" value="100"/>
 *   </broker-plugin>
 * </broker-plugins>
 */
public class SecuenciadorPlugin implements ActiveMQServerPlugin {

    private static final Logger logger = Logger.getLogger(SecuenciadorPlugin.class);
    
    // Propiedad del mensaje que contiene el número de secuencia
    private String secuenciaProperty = "secuencia";
    
    // Tamaño del buffer de mensajes para reordenamiento
    private int bufferSize = 100;
    
    // Buffer de mensajes por cola
    private final Map<String, List<MessageReference>> messageBuffers = new ConcurrentHashMap<>();
    
    @Override
    public void init(Map<String, String> properties) {
        logger.info("Inicializando SecuenciadorPlugin");
        
        if (properties != null) {
            if (properties.containsKey("secuenciaProperty")) {
                secuenciaProperty = properties.get("secuenciaProperty");
                logger.info("Propiedad de secuencia configurada: " + secuenciaProperty);
            }
            
            if (properties.containsKey("bufferSize")) {
                try {
                    bufferSize = Integer.parseInt(properties.get("bufferSize"));
                    logger.info("Tamaño de buffer configurado: " + bufferSize);
                } catch (NumberFormatException e) {
                    logger.warn("Valor inválido para bufferSize, usando valor por defecto: " + bufferSize);
                }
            }
        }
        
        logger.info("SecuenciadorPlugin inicializado correctamente");
    }

    @Override
    public void registered(ActiveMQServer server) {
        logger.info("SecuenciadorPlugin registrado en el servidor: " + server.getIdentity());
    }

    @Override
    public void unregistered(ActiveMQServer server) {
        logger.info("SecuenciadorPlugin desregistrado del servidor: " + server.getIdentity());
        messageBuffers.clear();
    }

    /**
     * Intercepta los mensajes antes de ser entregados al consumidor.
     * Reordena los mensajes según su número de secuencia.
     */
    @Override
    public void beforeDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {
        Queue queue = reference.getQueue();
        String queueName = queue.getName().toString();
        
        // Obtener el buffer de mensajes para esta cola
        List<MessageReference> buffer = messageBuffers.computeIfAbsent(
            queueName, 
            k -> new ArrayList<>()
        );
        
        // Agregar el mensaje al buffer
        synchronized (buffer) {
            buffer.add(reference);
            
            // Si el buffer alcanza el tamaño máximo, procesarlo
            if (buffer.size() >= bufferSize) {
                procesarBuffer(buffer);
            }
        }
    }

    /**
     * Procesa y reordena los mensajes en el buffer según su secuencia.
     */
    private void procesarBuffer(List<MessageReference> buffer) {
        if (buffer.isEmpty()) {
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
            
            logger.debug("Buffer procesado y ordenado con " + buffer.size() + " mensajes");
            
        } catch (Exception e) {
            logger.error("Error al procesar buffer de mensajes", e);
        } finally {
            buffer.clear();
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
            List<MessageReference> buffer = messageBuffers.get(queueName);
            
            if (buffer != null) {
                synchronized (buffer) {
                    procesarBuffer(buffer);
                }
            }
        }
    }
}
