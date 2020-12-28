/**
 *  MQTT Link Driver
 *
 * MIT License
 *
 * Copyright (c) 2020 license@mydevbox.com
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import groovy.json.JsonSlurper
import groovy.json.JsonOutput

public static String version() { return "v1.0.0" }
public static String rootTopic() { return "hubitat" }

//hubitat / {hub-name} / { device-name } / { device-capability } / STATE

metadata {
        definition(
        	name: "MQTT Link Driver",
        	namespace: "mydevbox",
        	author: "Chris Lawson, et al",
        	description: "A link between MQTT broker and MQTT Link app",
        	iconUrl: "https://s3.amazonaws.com/smartapp-icons/Connections/Cat-Connections.png",
        	iconX2Url: "https://s3.amazonaws.com/smartapp-icons/Connections/Cat-Connections@2x.png",
        	iconX3Url: "https://s3.amazonaws.com/smartapp-icons/Connections/Cat-Connections@3x.png"
        ) {
		capability "Notification"
        attribute "status", ""

		preferences {
			input(
		        name: "brokerIp", 
		        type: "string",
				title: "MQTT Broker IP Address",
				description: "e.g. 192.168.1.200",
				required: true,
				displayDuringSetup: true
			)
			input(
		        name: "brokerPort", 
		        type: "string",
				title: "MQTT Broker Port",
				description: "e.g. 1883",
				required: true,
				displayDuringSetup: true
			)

		    input(
		        name: "brokerUser", 
		        type: "string",
				title: "MQTT Broker Username",
				description: "e.g. mqtt_user",
				required: false,
				displayDuringSetup: true
			)
		    input(
		        name: "brokerPassword", 
		        type: "password",
				title: "MQTT Broker Password",
				description: "e.g. ^L85er1Z7g&%2En!",
				required: false,
				displayDuringSetup: true
			)
            input(
                name: "sendPayload", 
                type: "bool",
                title: "Send full payload messages on device events", 
                required: false, 
                default: false
            )
            input(
                name: "debugLogging", 
                type: "bool", 
                title: "Enable debug logging", 
                required: false, 
                default: false
            )
		}

        // Provided for broker setup and troubleshooting
		command "publish", [[name:"topic*",type:"STRING", title:"test",description:"Topic"],[name:"message",type:"STRING", description:"Message"]]
		command "subscribe",[[name:"topic*",type:"STRING", description:"Topic"]]
		command "unsubscribe",[[name:"topic*",type:"STRING", description:"Topic"]]
		command "connect"
		command "disconnect"
    }
}

void initialize() {
    debug("Initializing driver...")
    
    try {   
        interfaces.mqtt.connect(getBrokerUri(),
                           "hubitat_${getHubId()}", 
                           settings?.brokerUser, 
                           settings?.brokerPassword, 
                           lastWillTopic: "${getTopicPrefix()}LWT",
                           lastWillQos: 0, 
                           lastWillMessage: "offline", 
                           lastWillRetain: true)
       
        // delay for connection
        pauseExecution(1000)
        
    } catch(Exception e) {
        error("[initialize] ${e}")
    }
}

// ========================================================
// MQTT COMMANDS
// ========================================================

def publish(topic, payload) {
    publishMqtt(topic, payload)
}

def subscribe(topic) {
    debug("[subscribe] full topic: ${getTopicPrefix()}${topic}")
    interfaces.mqtt.subscribe("${getTopicPrefix()}${topic}")
}

def unsubscribe(topic) {
    debug("[unsubscribe] full topic: ${getTopicPrefix()}${topic}")
    interfaces.mqtt.unsubscribe("${getTopicPrefix()}${topic}")
}

def connect() {
    initialize()
}

def disconnect() {
    try {
        interfaces.mqtt.disconnect()
        disconnected()
    } catch(e) {
        warn("Disconnection from broker failed", ${e.message})
    }
}

// ========================================================
// MQTT LINK APP MESSAGE HANDLER
// ========================================================

// Device event notification from MQTT Link app via mqttLink.deviceNotification() 
def deviceNotification(message) {
    debug("[deviceNotification] Received message from MQTT Link app: '${message}'")
    
    
    def slurper = new JsonSlurper()
	def parsed = slurper.parseText(message)
    
    // Scheduled event in MQTT Broker app that renews device topic subs 
	if (parsed.path == '/subscribe') {
        deviceSubscribe(parsed)
	}
    
    // Device event 
	if (parsed.path == '/push') {      
        sendDeviceEvent(parsed.body)
	}
}

def deviceSubscribe(message) {
    
    // Clear all prior subsciptions
    if (message.update) {
        unsubscribe("#")
    }
    
    message.body.devices.each { key, capability ->
		capability.each { attribute ->
            def normalizedAttrib = normalize(attribute)
            def topic = "${normalizedAttrib}/${key}".toString()
            
            debug("[deviceSubscribe] topic: ${topic} attribute: ${attribute}")
            subscribe(topic)
		}
	}
}

def sendDeviceEvent(message) {
    topic = "${message.normalizedId}/"
    
    // Send command value only
    publishMqtt("${topic}${message.name}", message.value)
    
    if (message.pingRefresh) {
        return
    }
    
    if (settings.sendPayload) {
        // Send detailed event object
        publishMqtt("${topic}payload", JsonOutput.toJson(message))
    }
}

// ========================================================
// MQTT METHODS
// ========================================================

// Parse incoming message from the MQTT broker
def parse(String event) {
    def message = interfaces.mqtt.parseMessage(event)  
    def (name, hub, device, type) = message.topic.tokenize( '/' )
    
    debug("[parse] Received MQTT message: ${message}")
    
    def json = new groovy.json.JsonOutput().toJson([
        device: device,
        type: type,
        value: message.payload
	])
    
    return createEvent(name: "message", value: json, displayed: false)
}

def mqttClientStatus(status) {
    if (status.startsWith("Status: Connection succeeded")) 
        connected()
    else if (status.startsWith("Error: Connection lost"))
        disconnected()
    else if (status.startsWith("Error")) 
        error("[mqttClientStatus] ${status}")
    else
        info("[mqttClientStatus] ${status}")
}

def publishMqtt(topic, payload, qos = 0, retained = false) {
    def pubTopic = "${getTopicPrefix()}${topic}"

    try {
        interfaces.mqtt.publish("${pubTopic}", payload, qos, retained)
        debug("[publishMqtt] topic: ${pubTopic} payload: ${payload}")
        
    } catch (Exception e) {
        error("[publishMqtt] Unable to publish message: ${e}")
    }
}

// ========================================================
// ANNOUNCEMENTS
// ========================================================

def connected() {
    debug("[connected] Connected to broker")
    sendEvent (name: "status", value: "connected")
    announceLwtStatus("online")
    
    state?.reconnectDelay = 1
    state?.connectionActive = true
}

def disconnected() {
    debug("[disconnected] Disconnected from broker")
    sendEvent (name: "status", value: "disconnected")
    
    // first delay is 2 seconds, doubles every time
    state.reconnectDelay = (state.reconnectDelay ?: 1) * 2
    // don't def the delay get too crazy, max it out at 10 minutes
    if(state.reconnectDelay > 600) state.reconnectDelay = 600
    state?.connectionActive = false
    runIn(state?.reconnectDelay, initialize)
}

def announceLwtStatus(String status) {
    publishMqtt("LWT", status)
    publishMqtt("FW", "${location.hub.firmwareVersionString}")
    publishMqtt("IP", "${location.hub.localIP}")
    publishMqtt("UPTIME", "${location.hub.uptime}")
}

// ========================================================
// HELPERS
// ========================================================

def normalize(name) {
    return name.replaceAll("[^a-zA-Z0-9]+","-").toLowerCase()
}

def getBrokerUri() {        
    return "tcp://${settings?.brokerIp}:${settings?.brokerPort}"
}

def getHubId() {
    def hub = location.hubs[0]
    def hubNameNormalized = normalize(hub.name)
    return "${hubNameNormalized}-${hub.hardwareID}".toLowerCase()
}

def getTopicPrefix() {
    return "${rootTopic()}/${getHubId()}/"
}

def mqttConnected() {
    return interfaces.mqtt.isConnected()
}

def notMqttConnected() {
    return !mqttConnected()
}

// ========================================================
// LOGGING
// ========================================================

def debug(msg) {
	if (debugLogging) {
    	log.debug msg
    }
}

def info(msg) {
    log.info msg
}

def warn(msg) {
    log.warn msg
}

def error(msg) {
    log.error msg
}
