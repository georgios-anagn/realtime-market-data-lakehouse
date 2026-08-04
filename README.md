# Real-Time Market Data Lakehouse

Real-time streaming data platform using the Databricks Lakehouse architecture and the Medallion Architecture (Bronze → Silver → Gold). Visualized analytics with Databricks SQL.

## Draft Architecture
```mermaid
flowchart TD
    A["Market Data Provider<br/>(Finnhub)"]
    B["WebSocket Stream"]
    C["Python Ingestion"]
    D["JSON Files<br/>(Micro-batches)"]
    E["Unity Catalog Volume<br/>(Landing Zone)"]
    F["Databricks Auto Loader"]
    G["Bronze Delta Table<br/>(Raw Data)"]
    H["Structured Streaming<br/>Transformations"]
    I["Silver Delta Table<br/>(Clean & Validated Data)"]
    J["Business Aggregations<br/>(Window Functions)"]
    K["Gold Delta Tables<br/>(Analytics)"]
    L["Databricks SQL"]
    M["Live Dashboard"]

    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
    H --> I
    I --> J
    J --> K
    K --> L
    L --> M
```