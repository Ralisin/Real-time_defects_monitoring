# real-time monitoring of defects in Laser Powder Bed Fusion

## Architecture

```plaintext
   [ Server (exposes API) ] 
              ↑
              |  (Periodic GET requests)
              |
 [ Python Script (producer) ] 
              |
              |  (push)
              ↓
      [ Kafka (broker) ] 
              | 
              |  (pull) 
              ↓
     [ Flink (consumer) ]
```