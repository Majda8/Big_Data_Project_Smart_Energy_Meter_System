#include <PZEM004Tv30.h>
#include <LiquidCrystal_I2C.h>
#include <WiFi.h>
#include <HTTPClient.h>

#define PZEM_RX_PIN 16
#define PZEM_TX_PIN 17
#define PZEM_SERIAL Serial1
const int relay1 = 25;
const int relay2 = 26;
const char* ssid = "MAJDOULINE";
const char* password = "majdouline2002@";
const char* host = "http://192.168.51.232:5000/post-data"; // Adresse de notre serveur Flask

PZEM004Tv30 pzem(PZEM_SERIAL, PZEM_RX_PIN, PZEM_TX_PIN);
LiquidCrystal_I2C lcd(0x27, 16, 4);

void setup() {
    pinMode(relay1, OUTPUT);
    pinMode(relay2, OUTPUT);
    
    analogReadResolution(12);
    Serial.begin(115200);
    lcd.init();
    lcd.backlight();
    lcd.setCursor(0, 0);
    lcd.print("V:");
    lcd.setCursor(11, 0);
    lcd.print("I:");
    lcd.setCursor(0, 1);
    lcd.print("P:");
    lcd.setCursor(11, 1);
    lcd.print("E:");
    lcd.setCursor(0, 2);
    lcd.print("FP:");
    lcd.setCursor(11, 2);
    lcd.print("F:");

    // Connexion au WiFi
    Serial.println();
    Serial.print("Connecting to ");
    Serial.println(ssid);
    WiFi.begin(ssid, password);
    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        Serial.print(".");
    }
    Serial.println("");
    Serial.println("WiFi connected");
    Serial.println("IP address: ");
    Serial.println(WiFi.localIP());
}

void loop() {
    float V = pzem.voltage();
    float I = pzem.current();
    float P = pzem.power();
    float E = pzem.energy();
    float F = pzem.frequency();
    float FP = pzem.pf();

    

    // Affichage sur le moniteur série
    Serial.print("\n I=");
    Serial.print(I);
    Serial.print(" A");
    Serial.print("\n V=");
    Serial.print(V);
    Serial.print(" V");
    Serial.print("\n P=");
    Serial.print(P);
    Serial.print(" W");
    Serial.print("\n E=");
    Serial.print(E);
    Serial.print("KWh");
    Serial.print("\n FP=");
    Serial.print(FP);
    Serial.print("");
    Serial.print("\n F=");
    Serial.print(F);
    Serial.print("Hz");

    // Affichage sur l'écran LCD
    lcd.setCursor(2, 0);
    lcd.print(V, 0);
    lcd.print("V");
    lcd.setCursor(13, 0);
    lcd.print(I, 2);
    lcd.print("A");
    lcd.setCursor(2, 1);
    lcd.print(P, 2);
    lcd.print("W");
    lcd.setCursor(13, 1);
    lcd.print(E, 2);
    lcd.print("KWh");
    lcd.setCursor(3, 2);
    lcd.print(FP, 2);
    lcd.setCursor(13, 2);
    lcd.print(F, 1);
    lcd.print("Hz");

    // Envoi des données au serveur Flask
    if (WiFi.status() == WL_CONNECTED) {
        HTTPClient http;
        String url = String(host) + "?V=" + V + "&I=" + I + "&P=" + P + "&E=" + E + "&FP=" + FP + "&F=" + F;
        http.begin(url);  // Connecter à l'URL du serveur Flask
        int httpResponseCode = http.GET();  // Envoie de la requête GET
        
        if (httpResponseCode > 0) {
            String response = http.getString();
            Serial.println(httpResponseCode);
            Serial.println(response);
        } else {
            Serial.print("Error on sending POST: ");
            Serial.println(httpResponseCode);
        }
        http.end();
    } else {
        Serial.println("WiFi Disconnected");
    }
   delay(3000);
}
