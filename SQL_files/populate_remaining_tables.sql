-- Populate remaining empty AdventureWorks tables
-- Execute this script in Snowflake to fill all empty tables with sample data

-- PRODUCTION_PRODUCTDESCRIPTION (already has some data, adding more)
INSERT INTO Production_ProductDescription (Description) VALUES
('High-performance carbon fiber frame with aerodynamic design'),
('Lightweight aluminum construction for durability'),
('Professional-grade components for competitive cycling'),
('Weather-resistant materials for all-season use'),
('Ergonomic design for maximum comfort'),
('Advanced suspension system for smooth rides'),
('Precision-engineered for optimal performance'),
('Eco-friendly materials and manufacturing process'),
('Customizable options available for personalization'),
('Industry-leading warranty and support');

-- PRODUCTION_CULTURE (already has some data, adding more)
INSERT INTO Production_Culture (CultureID, Name) VALUES
('it', 'Italian'),
('ja', 'Japanese'),
('ko', 'Korean'),
('nl', 'Dutch'),
('no', 'Norwegian'),
('pl', 'Polish'),
('pt', 'Portuguese'),
('ru', 'Russian'),
('sv', 'Swedish'),
('th', 'Thai');

-- PRODUCTION_PRODUCTMODELPRODUCTDESCRIPTIONCULTURE
INSERT INTO Production_ProductModelProductDescriptionCulture (ProductModelID, ProductDescriptionID, CultureID) VALUES
(1, 1, 'en'), (1, 2, 'fr'), (1, 3, 'de'),
(2, 4, 'en'), (2, 5, 'es'), (2, 6, 'it'),
(3, 7, 'en'), (3, 8, 'ja'), (3, 9, 'ko'),
(4, 10, 'en'), (4, 11, 'pt'), (4, 12, 'ru'),
(5, 13, 'en'), (5, 14, 'sv'), (5, 15, 'th'),
(6, 16, 'en'), (6, 17, 'nl'), (6, 18, 'no'),
(7, 19, 'en'), (7, 20, 'pl'), (7, 1, 'fi'),
(8, 2, 'en'), (8, 3, 'da'), (8, 4, 'bg'),
(9, 5, 'en'), (9, 6, 'ca'), (9, 7, 'cs'),
(10, 8, 'en'), (10, 9, 'el'), (10, 10, 'ar');

-- PERSON_BUSINESSENTITYADDRESS
INSERT INTO Person_BusinessEntityAddress (BusinessEntityID, AddressID, AddressTypeID) VALUES
(1, 1, 1), (2, 2, 1), (3, 3, 1), (4, 4, 1), (5, 5, 1),
(6, 6, 1), (7, 7, 2), (8, 8, 2), (9, 9, 2), (10, 10, 2),
(11, 11, 3), (12, 12, 3), (13, 1, 4), (14, 2, 4), (15, 3, 5),
(16, 4, 5), (17, 5, 6), (18, 6, 6), (19, 7, 7), (20, 8, 7);

-- PERSON_BUSINESSENTITYCONTACT
INSERT INTO Person_BusinessEntityContact (BusinessEntityID, PersonID, ContactTypeID) VALUES
(16, 1, 1), (16, 2, 2), (17, 3, 3), (17, 4, 4), (18, 5, 5),
(18, 6, 6), (19, 7, 7), (19, 8, 8), (20, 9, 9), (20, 10, 10),
(16, 11, 1), (17, 12, 2), (18, 13, 3), (19, 14, 4), (20, 15, 5);

-- PRODUCTION_PRODUCTREVIEW
INSERT INTO Production_ProductReview (ProductID, ReviewerName, ReviewDate, EmailAddress, Rating, Comments) VALUES
(1, 'John Smith', '2023-01-15', 'john.smith@email.com', 5, 'Excellent product, highly recommended!'),
(2, 'Sarah Johnson', '2023-02-20', 'sarah.j@email.com', 4, 'Good quality, fast delivery'),
(3, 'Mike Wilson', '2023-03-10', 'mike.w@email.com', 5, 'Perfect for my needs'),
(4, 'Lisa Brown', '2023-04-05', 'lisa.b@email.com', 3, 'Average product, could be better'),
(5, 'David Lee', '2023-05-12', 'david.lee@email.com', 4, 'Good value for money'),
(6, 'Emma Davis', '2023-06-18', 'emma.d@email.com', 5, 'Outstanding quality and service'),
(7, 'Tom Anderson', '2023-07-22', 'tom.a@email.com', 4, 'Satisfied with the purchase'),
(8, 'Amy Taylor', '2023-08-30', 'amy.t@email.com', 5, 'Exceeded expectations'),
(9, 'Chris Martin', '2023-09-14', 'chris.m@email.com', 4, 'Reliable and durable'),
(10, 'Jessica White', '2023-10-25', 'jessica.w@email.com', 5, 'Will definitely buy again');

-- HUMANRESOURCES_JOBCANDIDATE
INSERT INTO HumanResources_JobCandidate (BusinessEntityID, Resume) VALUES
(NULL, '<Resume><Name>Alex Johnson</Name><Skills>SQL, Python, Data Analysis</Skills></Resume>'),
(NULL, '<Resume><Name>Maria Garcia</Name><Skills>Project Management, Agile</Skills></Resume>'),
(NULL, '<Resume><Name>Robert Chen</Name><Skills>Software Development, Java</Skills></Resume>'),
(NULL, '<Resume><Name>Jennifer Kim</Name><Skills>Marketing, Digital Strategy</Skills></Resume>'),
(NULL, '<Resume><Name>Michael Brown</Name><Skills>Sales, Customer Relations</Skills></Resume>');

-- PRODUCTION_PRODUCTCOSTHISTORY
INSERT INTO Production_ProductCostHistory (ProductID, StartDate, EndDate, StandardCost) VALUES
(1, '2023-01-01', '2023-06-30', 45.50),
(1, '2023-07-01', NULL, 47.25),
(2, '2023-01-01', '2023-06-30', 32.75),
(2, '2023-07-01', NULL, 34.00),
(3, '2023-01-01', '2023-06-30', 28.90),
(3, '2023-07-01', NULL, 30.15),
(4, '2023-01-01', '2023-06-30', 52.30),
(4, '2023-07-01', NULL, 54.80),
(5, '2023-01-01', '2023-06-30', 38.65),
(5, '2023-07-01', NULL, 40.20);

-- PRODUCTION_PRODUCTLISTPRICEHISTORY
INSERT INTO Production_ProductListPriceHistory (ProductID, StartDate, EndDate, ListPrice) VALUES
(1, '2023-01-01', '2023-06-30', 89.99),
(1, '2023-07-01', NULL, 94.99),
(2, '2023-01-01', '2023-06-30', 65.50),
(2, '2023-07-01', NULL, 69.99),
(3, '2023-01-01', '2023-06-30', 57.80),
(3, '2023-07-01', NULL, 61.30),
(4, '2023-01-01', '2023-06-30', 104.60),
(4, '2023-07-01', NULL, 109.80),
(5, '2023-01-01', '2023-06-30', 77.30),
(5, '2023-07-01', NULL, 81.45);

-- SALES_SALESORDERHEADERSALESREASON
INSERT INTO Sales_SalesOrderHeaderSalesReason (SalesOrderID, SalesReasonID) VALUES
(1, 1), (1, 3), (2, 2), (2, 4), (3, 5), (3, 7),
(4, 6), (4, 8), (5, 9), (5, 10), (6, 1), (6, 2),
(7, 3), (7, 4), (8, 5), (8, 6), (9, 7), (9, 8), (10, 9), (10, 10);

-- PRODUCTION_WORKORDERROUTING
INSERT INTO Production_WorkOrderRouting (WorkOrderID, ProductID, OperationSequence, LocationID, ScheduledStartDate, ScheduledEndDate, ActualStartDate, ActualEndDate, ActualResourceHrs, PlannedCost, ActualCost) VALUES
(1, 1, 1, 1, '2023-06-07', '2023-06-08', '2023-06-07', '2023-06-08', 8.0, 120.00, 125.50),
(1, 1, 2, 2, '2023-06-08', '2023-06-09', '2023-06-08', '2023-06-09', 6.0, 90.00, 88.75),
(2, 2, 1, 3, '2023-06-07', '2023-06-08', '2023-06-07', '2023-06-08', 7.5, 112.50, 115.25),
(2, 2, 2, 4, '2023-06-08', '2023-06-09', '2023-06-08', '2023-06-09', 5.5, 82.50, 85.00),
(3, 3, 1, 5, '2023-06-07', '2023-06-08', '2023-06-07', '2023-06-08', 9.0, 135.00, 138.75);

-- PRODUCTION_DOCUMENT
INSERT INTO Production_Document (DocumentNode, Title, Owner, FolderFlag, FileName, FileExtension, Revision, ChangeNumber, Status, DocumentSummary, Document) VALUES
('/1/', 'Product Specifications', 1, FALSE, 'ProductSpecs', 'pdf', '1.0', 1, 1, 'Detailed product specifications', TO_BINARY('255044462D312E34', 'HEX')),
('/2/', 'Assembly Instructions', 2, FALSE, 'Assembly', 'pdf', '2.1', 2, 1, 'Step-by-step assembly guide', TO_BINARY('255044462D312E34', 'HEX')),
('/3/', 'Safety Manual', 3, FALSE, 'Safety', 'pdf', '1.5', 1, 1, 'Safety guidelines and procedures', TO_BINARY('255044462D312E34', 'HEX')),
('/4/', 'Quality Standards', 4, FALSE, 'Quality', 'pdf', '3.0', 3, 1, 'Quality control standards', TO_BINARY('255044462D312E34', 'HEX')),
('/5/', 'Maintenance Guide', 5, FALSE, 'Maintenance', 'pdf', '1.2', 1, 1, 'Maintenance procedures', TO_BINARY('255044462D312E34', 'HEX'));

-- PRODUCTION_ILLUSTRATION
INSERT INTO Production_Illustration (Diagram) VALUES
('Product Assembly Diagram 1'),
('Product Assembly Diagram 2'),
('Product Assembly Diagram 3'),
('Product Assembly Diagram 4'),
('Product Assembly Diagram 5');

-- PRODUCTION_PRODUCTMODELILLUSTRATION
INSERT INTO Production_ProductModelIllustration (ProductModelID, IllustrationID) VALUES
(1, 1), (2, 2), (3, 3), (4, 4), (5, 5),
(6, 1), (7, 2), (8, 3), (9, 4), (10, 5);

-- PRODUCTION_PRODUCTPHOTO
INSERT INTO Production_ProductPhoto (ThumbNailPhoto, ThumbnailPhotoFileName, LargePhoto, LargePhotoFileName) VALUES
(TO_BINARY('89504E470D0A1A0A', 'HEX'), 'product1_thumb.jpg', TO_BINARY('89504E470D0A1A0A0000000D49484452', 'HEX'), 'product1_large.jpg'),
(TO_BINARY('89504E470D0A1A0B', 'HEX'), 'product2_thumb.jpg', TO_BINARY('89504E470D0A1A0A0000000D49484453', 'HEX'), 'product2_large.jpg'),
(TO_BINARY('89504E470D0A1A0C', 'HEX'), 'product3_thumb.jpg', TO_BINARY('89504E470D0A1A0A0000000D49484454', 'HEX'), 'product3_large.jpg'),
(TO_BINARY('89504E470D0A1A0D', 'HEX'), 'product4_thumb.jpg', TO_BINARY('89504E470D0A1A0A0000000D49484455', 'HEX'), 'product4_large.jpg'),
(TO_BINARY('89504E470D0A1A0E', 'HEX'), 'product5_thumb.jpg', TO_BINARY('89504E470D0A1A0A0000000D49484456', 'HEX'), 'product5_large.jpg');

-- PRODUCTION_PRODUCTPRODUCTPHOTO
INSERT INTO Production_ProductProductPhoto (ProductID, ProductPhotoID, Primary) VALUES
(1, 1, TRUE), (2, 2, TRUE), (3, 3, TRUE), (4, 4, TRUE), (5, 5, TRUE),
(6, 1, FALSE), (7, 2, FALSE), (8, 3, FALSE), (9, 4, FALSE), (10, 5, FALSE);

-- PRODUCTION_PRODUCTDOCUMENT
INSERT INTO Production_ProductDocument (ProductID, DocumentNode) VALUES
(1, '/1/'), (2, '/2/'), (3, '/3/'), (4, '/4/'), (5, '/5/'),
(6, '/1/'), (7, '/2/'), (8, '/3/'), (9, '/4/'), (10, '/5/');

-- PRODUCTION_TRANSACTIONHISTORY
INSERT INTO Production_TransactionHistory (ProductID, ReferenceOrderID, ReferenceOrderLineID, TransactionDate, TransactionType, Quantity, ActualCost) VALUES
(1, 1, 1, '2023-06-07', 'W', 8, 125.50),
(2, 2, 2, '2023-06-07', 'W', 8, 115.25),
(3, 3, 3, '2023-06-07', 'W', 6, 138.75),
(4, 4, 4, '2023-06-07', 'W', 6, 142.30),
(5, 5, 5, '2023-06-07', 'W', 4, 98.45),
(1, 1, 1, '2023-06-08', 'S', -1, 0.00),
(2, 2, 2, '2023-06-08', 'S', -3, 0.00),
(3, 3, 3, '2023-06-08', 'S', -1, 0.00),
(4, 4, 4, '2023-06-08', 'S', -1, 0.00),
(5, 5, 5, '2023-06-08', 'S', -2, 0.00);

-- PRODUCTION_TRANSACTIONHISTORYARCHIVE
INSERT INTO Production_TransactionHistoryArchive (TransactionID, ProductID, ReferenceOrderID, ReferenceOrderLineID, TransactionDate, TransactionType, Quantity, ActualCost) VALUES
(1, 1, 1, 1, '2022-06-07', 'W', 10, 120.00),
(2, 2, 2, 2, '2022-06-07', 'W', 10, 110.00),
(3, 3, 3, 3, '2022-06-07', 'W', 8, 135.00),
(4, 4, 4, 4, '2022-06-07', 'W', 8, 140.00),
(5, 5, 5, 5, '2022-06-07', 'W', 6, 95.00);

-- PRODUCTION_BILLOFMATERIALS
INSERT INTO Production_BillOfMaterials (ProductAssemblyID, ComponentID, StartDate, EndDate, UnitMeasureCode, BOMLevel, PerAssemblyQty) VALUES
(1, 2, '2023-01-01', NULL, 'EA', 1, 2.00),
(1, 3, '2023-01-01', NULL, 'EA', 1, 1.00),
(2, 4, '2023-01-01', NULL, 'EA', 1, 1.00),
(2, 5, '2023-01-01', NULL, 'EA', 1, 4.00),
(3, 6, '2023-01-01', NULL, 'EA', 1, 2.00),
(4, 7, '2023-01-01', NULL, 'EA', 1, 1.00),
(4, 8, '2023-01-01', NULL, 'EA', 1, 2.00),
(5, 9, '2023-01-01', NULL, 'EA', 1, 1.00),
(5, 10, '2023-01-01', NULL, 'EA', 1, 1.00),
(6, 11, '2023-01-01', NULL, 'EA', 1, 3.00);

-- PURCHASING_PRODUCTVENDOR
INSERT INTO Purchasing_ProductVendor (ProductID, BusinessEntityID, AverageLeadTime, StandardPrice, LastReceiptCost, LastReceiptDate, MinOrderQty, MaxOrderQty, OnOrderQty, UnitMeasureCode) VALUES
(1, 16, 14, 45.50, 44.75, '2023-10-15', 10, 1000, 0, 'EA'),
(2, 17, 10, 32.75, 33.00, '2023-10-20', 20, 500, 50, 'EA'),
(3, 18, 12, 28.90, 29.15, '2023-10-18', 15, 750, 25, 'EA'),
(4, 19, 16, 52.30, 51.85, '2023-10-12', 8, 400, 0, 'EA'),
(5, 20, 8, 38.65, 39.20, '2023-10-25', 12, 600, 75, 'EA'),
(6, 16, 15, 41.20, 40.95, '2023-10-14', 10, 800, 30, 'EA'),
(7, 17, 11, 35.80, 36.10, '2023-10-22', 18, 450, 0, 'EA'),
(8, 18, 13, 31.45, 31.70, '2023-10-16', 14, 650, 40, 'EA'),
(9, 19, 17, 48.90, 48.50, '2023-10-11', 6, 350, 20, 'EA'),
(10, 20, 9, 42.15, 42.80, '2023-10-26', 11, 550, 60, 'EA');

-- SALES_SALESTERRITORYHISTORY
INSERT INTO Sales_SalesTerritoryHistory (BusinessEntityID, TerritoryID, StartDate, EndDate) VALUES
(1, 1, '2022-01-01', '2023-06-30'),
(1, 2, '2023-07-01', NULL),
(2, 4, '2021-01-01', NULL),
(3, 3, '2020-01-01', NULL),
(4, 6, '2019-01-01', NULL),
(5, 5, '2018-01-01', NULL);

-- SALES_SALESPERSONQUOTAHISTORY
INSERT INTO Sales_SalesPersonQuotaHistory (BusinessEntityID, QuotaDate, SalesQuota) VALUES
(1, '2023-01-01', 300000.00),
(1, '2023-07-01', 320000.00),
(2, '2023-01-01', 300000.00),
(2, '2023-07-01', 315000.00),
(3, '2023-01-01', 300000.00),
(3, '2023-07-01', 310000.00),
(4, '2023-01-01', 300000.00),
(4, '2023-07-01', 325000.00),
(5, '2023-01-01', 300000.00),
(5, '2023-07-01', 305000.00);

-- PERSON_PASSWORD
INSERT INTO Person_Password (BusinessEntityID, PasswordHash, PasswordSalt) VALUES
(1, 'pbFwXWE99Lp/YPCC6gUKUd5Mo5w=', 'bE3XiWw='),
(2, 'uBFwXWE99Lp/YPCC6gUKUd5Mo5w=', 'cF4YjXx='),
(3, 'vCGxYXF00Mp/ZQDD7hVLVe6Np6x=', 'dG5ZkYy='),
(4, 'wDHyZYG11Nq/aREE8iWMWf7Oq7y=', 'eH6akZz='),
(5, 'xEIzaZH22Or/bSFF9jXNXg8Pr8z=', 'fI7blA0='),
(6, 'yFJ0baI33Ps/cTGG0kYOYh9Qs90=', 'gJ8cmB1='),
(7, 'zGK1cbJ44Qt/dUHH1lZPZi0Rt01=', 'hK9dnC2='),
(8, 'AHL2dcK55Ru/eVII2maQaj1Su12=', 'iL0eoD3='),
(9, 'BIM3edL66Sv/fWJJ3nbRbk2Tv23=', 'jM1fpE4='),
(10, 'CJN4feM77Tw/gXKK4ocSck3Uw34=', 'kN2gqF5=');

-- SALES_PERSONCREDITCARD
INSERT INTO Sales_PersonCreditCard (BusinessEntityID, CreditCardID) VALUES
(7, 1), (8, 2), (9, 3), (10, 4), (11, 5),
(12, 6), (13, 7), (14, 8), (15, 9), (7, 10);

-- SALES_SHOPPINGCARTITEM
INSERT INTO Sales_ShoppingCartItem (ShoppingCartID, Quantity, ProductID, DateCreated) VALUES
('user1_cart', 2, 1, '2023-11-01 10:30:00'),
('user1_cart', 1, 3, '2023-11-01 10:35:00'),
('user2_cart', 3, 2, '2023-11-01 11:15:00'),
('user2_cart', 1, 5, '2023-11-01 11:20:00'),
('user3_cart', 1, 4, '2023-11-01 14:45:00'),
('user4_cart', 2, 6, '2023-11-01 15:30:00'),
('user4_cart', 1, 7, '2023-11-01 15:35:00'),
('user5_cart', 4, 8, '2023-11-01 16:20:00'),
('user6_cart', 1, 9, '2023-11-01 17:10:00'),
('user6_cart', 2, 10, '2023-11-01 17:15:00');

-- SALES_SALESTAXRATE
INSERT INTO Sales_SalesTaxRate (StateProvinceID, TaxType, TaxRate, Name) VALUES
(1, 1, 8.75, 'Alabama State Sales Tax'),
(2, 1, 0.00, 'Alaska State Sales Tax'),
(3, 1, 5.60, 'Arizona State Sales Tax'),
(4, 1, 6.50, 'Arkansas State Sales Tax'),
(5, 1, 7.25, 'California State Sales Tax'),
(6, 1, 2.90, 'Colorado State Sales Tax'),
(7, 1, 6.35, 'Connecticut State Sales Tax'),
(8, 1, 0.00, 'Delaware State Sales Tax'),
(9, 1, 6.00, 'Florida State Sales Tax'),
(10, 1, 4.00, 'Georgia State Sales Tax'),
(11, 1, 13.00, 'Ontario Provincial Sales Tax'),
(12, 1, 12.00, 'British Columbia Provincial Sales Tax');

COMMIT;