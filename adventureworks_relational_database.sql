
-- Create Full-Text Search Indexes
-- Store precomputed tsvector values to optimize search performance.
CREATE TABLE Production.ProductDescriptionIndex AS
SELECT
    pmdc.ProductDescriptionID,
    pd.Description,
    to_tsvector('english', pd.Description) AS tsv
FROM Production.ProductModelProductDescriptionCulture pmdc
JOIN Production.ProductDescription pd
    ON pmdc.ProductDescriptionID = pd.ProductDescriptionID
WHERE pmdc.CultureID = 'en';  -- Only English descriptions

-- Create indexes to speed up full-text search
CREATE INDEX idx_productdescription_tsv ON Production.ProductDescriptionIndex USING GIN(tsv);


CREATE TABLE Production.ProductIDKeywords_split AS
WITH exploded_keywords AS (
    -- split keywords into multiple rows
    SELECT
        p.ProductID,
        pd.ProductDescriptionID,
        unnest(string_to_array(pd.Keywords, ', ')) AS keyword
    FROM Production.ProductDescriptionKeywords pd
    JOIN Production.ProductModelProductDescriptionCulture pmdc
        ON pd.ProductDescriptionID = pmdc.ProductDescriptionID
    JOIN Production.ProductModel pm
        ON pmdc.ProductModelID = pm.ProductModelID
    JOIN Production.Product p
        ON pm.ProductModelID = p.ProductModelID
    WHERE pmdc.CultureID = 'en'
),
ranked_keywords AS (
    SELECT
        ProductID,
        ProductDescriptionID,
        keyword,
        ROW_NUMBER() OVER (PARTITION BY ProductDescriptionID ORDER BY keyword) AS rn
    FROM exploded_keywords
    WHERE keyword IS NOT NULL
)
SELECT
    ProductID,
    ProductDescriptionID,
    keyword
FROM ranked_keywords
WHERE rn <= 10  -- keep 10 keywords
ORDER BY ProductID, ProductDescriptionID, rn;

CREATE TABLE --- AS
SELECT
    SalesOrderID,
    RevisionNumber,
    OrderDate,
    DueDate,
    ShipDate,
    Status,
    OnlineOrderFlag,
    SalesOrderNumber,
    PurchaseOrderNumber,
    AccountNumber,
    CustomerID,
   	.
    (ShipDate - OrderDate) AS Duration  -- Calculate duration in days
FROM ;

--------------------------------------------------------------
-- identify the Customer Who Purchased the Most Bikes
-- Find the customer with the highest number of bike purchases.

WITH bike_purchase_counts AS (
    SELECT h.CustomerID, COUNT(d.ProductID) AS total_bike_purchases
    FROM Sales.FilteredSalesOrderHeader h
    JOIN Sales.FilteredSalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
    JOIN Production.Product p ON d.ProductID = p.ProductID
    JOIN Production.ProductSubcategory ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
    JOIN Production.ProductCategory pc ON ps.ProductCategoryID = pc.ProductCategoryID
    WHERE pc.Name = 'Bikes'
    GROUP BY h.CustomerID
)
SELECT *
FROM bike_purchase_counts
ORDER BY total_bike_purchases DESC
LIMIT 1;

--------------------------------------------------------------
-- Get the Most Recently Purchased Product for Customer 29715
-- Retrieve the latest purchased product to use as a basis for recommendations.

WITH recent_purchase AS (
  SELECT d.ProductID
  FROM Sales.FilteredSalesOrderHeader h
  JOIN Sales.FilteredSalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
  WHERE h.CustomerID = 29715
  ORDER BY h.OrderDate DESC
  LIMIT 1
)
SELECT * FROM recent_purchase;

--------------------------------------------------------------
-- Find Similar Products Based on Description (Content-Based Filtering)
-- Retrieve products similar to the most recently purchased one based on textual similarity.

WITH recent_purchase AS (
  SELECT d.ProductID
  FROM Sales.FilteredSalesOrderHeader h
  JOIN Sales.FilteredSalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
  WHERE h.CustomerID = 29715
  ORDER BY h.OrderDate DESC
  LIMIT 1
),
target AS (
  -- Extract the description of the target product and convert it into a tsquery.
  SELECT pdi.ProductDescriptionID,
         to_tsquery('english', string_agg(word, ' | ')) AS desc_query
  FROM recent_purchase rp
  JOIN Production.Product p ON p.ProductID = rp.ProductID
  JOIN Production.ProductModel pm ON p.ProductModelID = pm.ProductModelID
  JOIN Production.ProductModelProductDescriptionCulture pc
       ON pm.ProductModelID = pc.ProductModelID AND pc.CultureID = 'en'
  JOIN Production.ProductDescriptionIndex pdi
       ON pc.ProductDescriptionID = pdi.ProductDescriptionID,
       LATERAL (
         SELECT unnest(
           string_to_array(
             regexp_replace(pdi.tsv::text, E'\'([^\']+)\':\\d+', E'\\1', 'g'),
             ' '
           )
         ) AS word
       ) words
  WHERE length(word) > 2
  GROUP BY pdi.ProductDescriptionID
)
SELECT p.ProductID, p.Name, pdi.Description
FROM Production.Product p
JOIN Production.ProductModel pm ON p.ProductModelID = pm.ProductModelID
JOIN Production.ProductModelProductDescriptionCulture pc
       ON pm.ProductModelID = pc.ProductModelID AND pc.CultureID = 'en'
JOIN Production.ProductDescriptionIndex pdi
       ON pc.ProductDescriptionID = pdi.ProductDescriptionID
JOIN target t ON TRUE
WHERE pdi.tsv @@ t.desc_query
  AND p.ProductID <> (SELECT ProductID FROM recent_purchase)
ORDER BY ts_rank_cd(pdi.tsv, t.desc_query) DESC
LIMIT 10;

--------------------------------------------------------------
-- Diversified Recommendations (Bikes + Accessories)
-- Recommend products from multiple categories, not just similar bikes.

WITH recent_purchase AS (
  SELECT d.ProductID
  FROM Sales.FilteredSalesOrderHeader h
  JOIN Sales.FilteredSalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
  WHERE h.CustomerID = 29715
  ORDER BY h.OrderDate DESC
  LIMIT 1
),
target AS (
  -- Extract product description and generate tsquery.
  SELECT pdi.ProductDescriptionID,
         to_tsquery('english', string_agg(word, ' | ')) AS desc_query
  FROM recent_purchase rp
  JOIN Production.Product p ON p.ProductID = rp.ProductID
  JOIN Production.ProductModel pm ON p.ProductModelID = pm.ProductModelID
  JOIN Production.ProductModelProductDescriptionCulture pmppc
       ON pm.ProductModelID = pmppc.ProductModelID AND pmppc.CultureID = 'en'
  JOIN Production.ProductDescriptionIndex pdi
       ON pmppc.ProductDescriptionID = pdi.ProductDescriptionID,
       LATERAL (
         SELECT unnest(
           string_to_array(
             regexp_replace(pdi.tsv::text, E'\'([^\']+)\':\\d+', E'\\1', 'g'),
             ' '
           )
         ) AS word
       ) words
  WHERE length(word) > 2
  GROUP BY pdi.ProductDescriptionID
)
SELECT p.ProductID, p.Name, pdi.Description, pc.Name AS CategoryName
FROM Production.Product p
JOIN Production.ProductModel pm ON p.ProductModelID = pm.ProductModelID
JOIN Production.ProductModelProductDescriptionCulture pmppc
       ON pm.ProductModelID = pmppc.ProductModelID AND pmppc.CultureID = 'en'
JOIN Production.ProductDescriptionIndex pdi
       ON pmppc.ProductDescriptionID = pdi.ProductDescriptionID
JOIN Production.ProductSubcategory ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
JOIN Production.ProductCategory pc ON ps.ProductCategoryID = pc.ProductCategoryID
JOIN target t ON TRUE
WHERE pdi.tsv @@ t.desc_query
  AND p.ProductID <> (SELECT ProductID FROM recent_purchase)
  AND pc.Name IN ('Bikes', 'Accessories')
ORDER BY ts_rank_cd(pdi.tsv, t.desc_query) DESC
LIMIT 20;

--------------------------------------------------------------
-- Collaborative Filtering: “Customers Who Bought This Also Bought”
-- Identify frequently co-purchased products.

WITH recent_purchase AS (
  SELECT d.ProductID
  FROM Sales.FilteredSalesOrderHeader h
  JOIN Sales.FilteredSalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
  WHERE h.CustomerID = 29715
  ORDER BY h.OrderDate DESC
  LIMIT 1
),
target_orders AS (
  -- Retrieve all orders containing the target product.
  SELECT DISTINCT h.SalesOrderID
  FROM Sales.FilteredSalesOrderHeader h
  JOIN Sales.FilteredSalesOrderDetail d
      ON h.SalesOrderID = d.SalesOrderID
  WHERE d.ProductID = (SELECT ProductID FROM recent_purchase)
),
co_purchases AS (
  -- Count occurrences of other products in the same orders.
  SELECT d.ProductID, COUNT(*) AS purchase_count
  FROM Sales.FilteredSalesOrderDetail d
  JOIN target_orders t ON d.SalesOrderID = t.SalesOrderID
  WHERE d.ProductID <> (SELECT ProductID FROM recent_purchase)
  GROUP BY d.ProductID
)
SELECT p.ProductID, p.Name, cp.purchase_count
FROM co_purchases cp
JOIN Production.Product p ON cp.ProductID = p.ProductID
ORDER BY cp.purchase_count DESC
LIMIT 20;


-- recommendation optimize
-- Split co-purchases into separate categories with Score Calculation

WITH recent_purchase AS (
    -- Identify the most recently purchased product by CustomerID = 29715
    SELECT d.ProductID
    FROM Sales.FilteredSalesOrderHeader h
    JOIN Sales.FilteredSalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
    WHERE h.CustomerID = 29715
    ORDER BY h.OrderDate DESC
    LIMIT 1
),
target_orders AS (
    -- Find all orders that contain the target product
    SELECT DISTINCT h.SalesOrderID
    FROM Sales.FilteredSalesOrderHeader h
    JOIN Sales.FilteredSalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
    WHERE d.ProductID = (SELECT ProductID FROM recent_purchase)
),
co_purchases AS (
    -- Count how many times other products co-occurred with the target product in the same orders
    SELECT d.ProductID, COUNT(*) AS purchase_count
    FROM Sales.FilteredSalesOrderDetail d
    JOIN target_orders t ON d.SalesOrderID = t.SalesOrderID
    WHERE d.ProductID <> (SELECT ProductID FROM recent_purchase)
    GROUP BY d.ProductID
)
SELECT
    pc.Name as category,
    p.ProductID,
    p.Name,
    co.purchase_count,
    -- Assign different weight values based on category and compute score
    CASE
        WHEN pc.Name = 'Bikes' THEN co.purchase_count * 1.0  -- Standard weight for bikes
        WHEN pc.Name = 'Accessories' THEN co.purchase_count * 1.2  -- Higher weight for accessories
        ELSE co.purchase_count * 0.8  -- Lower weight for other categories
    END AS score
FROM co_purchases co
JOIN Production.Product p ON co.ProductID = p.ProductID
JOIN Production.ProductSubcategory ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
JOIN Production.ProductCategory pc ON ps.ProductCategoryID = pc.ProductCategoryID
ORDER BY score DESC, co.purchase_count DESC;


