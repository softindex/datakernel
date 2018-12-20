## Global-Common

This module is a foundation for other Global components and includes:
* `DiscoveryService` - a special service that conducts a DNS-like role storing information about IPs of Master servers and
Public Keys associated with them. Basically, the main functions of `DiscoveryService`  are to resolve clients' requests 
for data location and registers newly uploaded data along with checking its digital signatures.
* Tools to manage Private and Public keys.
* Encryption and digital signature tools.
* Some other common classes
