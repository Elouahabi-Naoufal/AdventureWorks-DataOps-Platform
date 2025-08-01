-- Adventure Works Sample Data Population Script
-- Execute this script in Snowflake to populate all tables with sample data

-- Core reference data first
INSERT INTO Person_CountryRegion (CountryRegionCode, Name) VALUES
('USA', 'United States'),
('CAN', 'Canada'),
('GBR', 'United Kingdom'),
('DEU', 'Germany'),
('FRA', 'France'),
('JPN', 'Japan'),
('AUS', 'Australia'),
('BRA', 'Brazil'),
('CHN', 'China'),
('IND', 'India'),
('MEX', 'Mexico'),
('ITA', 'Italy');

INSERT INTO Sales_Currency (CurrencyCode, Name) VALUES
('USD', 'US Dollar'),
('CAD', 'Canadian Dollar'),
('GBP', 'British Pound'),
('EUR', 'Euro'),
('JPY', 'Japanese Yen'),
('AUD', 'Australian Dollar'),
('BRL', 'Brazilian Real'),
('CNY', 'Chinese Yuan'),
('INR', 'Indian Rupee'),
('MXN', 'Mexican Peso');

INSERT INTO Sales_SalesTerritory (Name, CountryRegionCode, "Group", SalesYTD, SalesLastYear, CostYTD, CostLastYear) VALUES
('Northwest', 'USA', 'North America', 7887186.7882, 3298694.4938, 684649.9621, 250187.8739),
('Northeast', 'USA', 'North America', 2402176.8476, 3607148.9371, 237144.0681, 340164.5938),
('Central', 'USA', 'North America', 3072175.118, 3205014.0767, 315278.9494, 320276.8789),
('Southwest', 'USA', 'North America', 10510853.8739, 8065040.1287, 1108914.8841, 846039.9923),
('Southeast', 'USA', 'North America', 2538667.2515, 3925071.4318, 271267.2823, 353239.9474),
('Canada', 'CAN', 'North America', 6771829.1376, 5693988.86, 710053.27, 565041.9435),
('France', 'FRA', 'Europe', 4772398.3078, 2396539.7601, 407246.4118, 181692.9847),
('Germany', 'DEU', 'Europe', 3805202.3478, 1307949.7917, 324639.38, 134219.7071),
('Australia', 'AUS', 'Pacific', 5977814.9154, 2278548.9776, 588268.6632, 267081.0543),
('United Kingdom', 'GBR', 'Europe', 5012905.3656, 1635823.3967, 508635.5639, 174686.1289);

INSERT INTO Person_StateProvince (StateProvinceCode, CountryRegionCode, Name, TerritoryID) VALUES
('AL', 'USA', 'Alabama', 5),
('AK', 'USA', 'Alaska', 1),
('AZ', 'USA', 'Arizona', 4),
('AR', 'USA', 'Arkansas', 3),
('CA', 'USA', 'California', 4),
('CO', 'USA', 'Colorado', 3),
('CT', 'USA', 'Connecticut', 2),
('DE', 'USA', 'Delaware', 2),
('FL', 'USA', 'Florida', 5),
('GA', 'USA', 'Georgia', 5),
('ON', 'CAN', 'Ontario', 6),
('BC', 'CAN', 'British Columbia', 6);

INSERT INTO Person_AddressType (Name) VALUES
('Billing'),
('Home'),
('Main Office'),
('Primary'),
('Shipping'),
('Archive'),
('Business'),
('Contact'),
('General'),
('International');

INSERT INTO Person_ContactType (Name) VALUES
('Accounting Manager'),
('Assistant Sales Agent'),
('Assistant Sales Representative'),
('Coordinator Foreign Markets'),
('Export Administrator'),
('International Marketing Manager'),
('Marketing Assistant'),
('Marketing Manager'),
('Marketing Representative'),
('Order Administrator');

INSERT INTO Person_PhoneNumberType (Name) VALUES
('Cell'),
('Home'),
('Work'),
('Other'),
('Fax'),
('Mobile'),
('Pager'),
('Emergency'),
('Business'),
('Personal');

INSERT INTO Production_UnitMeasure (UnitMeasureCode, Name) VALUES
('BOX', 'Boxes'),
('BTL', 'Bottle'),
('C', 'Container'),
('CM', 'Centimeter'),
('DZ', 'Dozen'),
('EA', 'Each'),
('G', 'Gram'),
('IN', 'Inch'),
('KG', 'Kilogram'),
('L', 'Liter'),
('LB', 'Pound'),
('M', 'Meter');

INSERT INTO Production_ProductCategory (Name) VALUES
('Bikes'),
('Components'),
('Clothing'),
('Accessories'),
('Electronics'),
('Tools'),
('Safety'),
('Maintenance'),
('Parts'),
('Upgrades');

INSERT INTO Production_ProductSubcategory (ProductCategoryID, Name) VALUES
(1, 'Mountain Bikes'),
(1, 'Road Bikes'),
(1, 'Touring Bikes'),
(2, 'Handlebars'),
(2, 'Bottom Brackets'),
(2, 'Brakes'),
(3, 'Bib-Shorts'),
(3, 'Caps'),
(3, 'Gloves'),
(4, 'Bike Racks'),
(4, 'Bike Stands'),
(4, 'Bottles and Cages');

INSERT INTO Production_ProductModel (Name, CatalogDescription, Instructions) VALUES
('Classic Vest', 'Our lightest and most breathable vest', 'Machine wash cold'),
('Cycling Cap', 'Lightweight cap for all weather', 'Hand wash recommended'),
('Full-Finger Gloves', 'Maximum grip and protection', 'Air dry only'),
('Half-Finger Gloves', 'Breathable summer gloves', 'Machine washable'),
('Headlights', 'High-intensity LED headlights', 'Battery operated'),
('Helmet', 'Safety certified bicycle helmet', 'One size fits most'),
('Hydration Pack', 'Lightweight hydration system', 'BPA-free materials'),
('Jersey', 'Professional cycling jersey', 'Moisture-wicking fabric'),
('Lights', 'Front and rear light set', 'Rechargeable battery'),
('Mountain Bike Socks', 'Cushioned performance socks', 'Synthetic blend');

INSERT INTO Production_Location (Name, CostRate, Availability) VALUES
('Debur and Polish', 14.50, 120.00),
('Final Assembly', 12.25, 120.00),
('Frame Forming', 15.75, 120.00),
('Frame Welding', 25.00, 120.00),
('Gear Assembly', 11.50, 120.00),
('Paint', 15.75, 120.00),
('Production', 0.00, 0.00),
('Quality Assurance', 18.00, 120.00),
('Specialized Paint', 18.00, 80.00),
('Subassembly', 12.25, 120.00);

INSERT INTO Production_ScrapReason (Name) VALUES
('Brake assembly not within design specifications'),
('Color incorrect'),
('Drill pattern incorrect'),
('Excess adhesive on part'),
('Incorrect bending angle'),
('Incorrect dimensions'),
('Machine malfunction'),
('Operator error'),
('Part not properly anodized'),
('Wheel alignment incorrect');

INSERT INTO HumanResources_Shift (Name, StartTime, EndTime) VALUES
('Day', '07:00:00', '15:00:00'),
('Evening', '15:00:00', '23:00:00'),
('Night', '23:00:00', '07:00:00');

INSERT INTO HumanResources_Department (Name, GroupName) VALUES
('Engineering', 'Research and Development'),
('Tool Design', 'Research and Development'),
('Sales', 'Sales and Marketing'),
('Marketing', 'Sales and Marketing'),
('Purchasing', 'Inventory Management'),
('Research and Development', 'Research and Development'),
('Production', 'Manufacturing'),
('Production Control', 'Manufacturing'),
('Human Resources', 'Executive General and Administration'),
('Finance', 'Executive General and Administration'),
('Information Services', 'Executive General and Administration'),
('Document Control', 'Quality Assurance'),
('Quality Assurance', 'Quality Assurance'),
('Facilities and Maintenance', 'Executive General and Administration'),
('Shipping and Receiving', 'Inventory Management'),
('Executive', 'Executive General and Administration');

INSERT INTO Purchasing_ShipMethod (Name, ShipBase, ShipRate) VALUES
('XRQ - TRUCK GROUND', 8.99, 1.99),
('ZY - EXPRESS', 9.99, 1.99),
('OVERSEAS - DELUXE', 29.99, 2.99),
('OVERNIGHT J-FAST', 21.99, 1.29),
('CARGO TRANSPORT 5', 8.99, 1.95);

INSERT INTO Sales_SalesReason (Name, ReasonType) VALUES
('Price', 'Other'),
('On Promotion', 'Promotion'),
('Magazine Advertisement', 'Marketing'),
('Television Advertisement', 'Marketing'),
('Manufacturer', 'Other'),
('Review', 'Other'),
('Demo Event', 'Marketing'),
('Sponsorship', 'Marketing'),
('Quality', 'Other'),
('Other', 'Other');

INSERT INTO Sales_SpecialOffer (Description, DiscountPct, Type, Category, StartDate, EndDate, MinQty, MaxQty) VALUES
('No Discount', 0.00, 'No Discount', 'No Discount', '2011-05-01', '2014-11-30', 0, NULL),
('Volume Discount 11 to 14', 0.02, 'Volume Discount', 'Reseller', '2012-05-01', '2014-11-30', 11, 14),
('Volume Discount 15 to 24', 0.05, 'Volume Discount', 'Reseller', '2012-05-01', '2014-11-30', 15, 24),
('Volume Discount 25 to 40', 0.10, 'Volume Discount', 'Reseller', '2012-05-01', '2014-11-30', 25, 40),
('Volume Discount 41 to 60', 0.15, 'Volume Discount', 'Reseller', '2012-05-01', '2014-11-30', 41, 60),
('Mountain-100 Clearance Sale', 0.35, 'Discontinued Product', 'Customer', '2012-07-01', '2012-07-31', 0, NULL),
('Sport Helmet Discount-2002', 0.10, 'Seasonal Discount', 'Customer', '2011-06-01', '2012-05-30', 0, NULL),
('Road-650 Overstock', 0.30, 'Excess Inventory', 'Customer', '2012-06-01', '2012-06-30', 0, NULL),
('Mountain Tire Sale', 0.50, 'Seasonal Discount', 'Customer', '2011-06-01', '2012-05-30', 0, NULL),
('LL Road Frame Sale', 0.35, 'Excess Inventory', 'Customer', '2012-06-01', '2012-06-30', 0, NULL);

INSERT INTO Sales_CreditCard (CardType, CardNumber, ExpMonth, ExpYear) VALUES
('SuperiorCard', '33332664695310', 11, 2006),
('Distinguish', '55552127249722', 8, 2005),
('ColonialVoice', '77778344838353', 7, 2005),
('ColonialVoice', '77774915718248', 7, 2006),
('Vista', '11114404600042', 4, 2005),
('Vista', '11115565328140', 4, 2006),
('SuperiorCard', '33331835013166', 4, 2006),
('SuperiorCard', '33332537043166', 3, 2006),
('Distinguish', '55553322172808', 11, 2005),
('Vista', '11111462225358', 8, 2005);

-- Business Entities (base for persons, stores, vendors)
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;
INSERT INTO Person_BusinessEntity DEFAULT VALUES;

-- Persons
INSERT INTO Person_Person (BusinessEntityID, PersonType, FirstName, LastName, EmailPromotion) VALUES
(1, 'EM', 'Ken', 'Sánchez', 0),
(2, 'EM', 'Terri', 'Duffy', 1),
(3, 'EM', 'Roberto', 'Tamburello', 0),
(4, 'EM', 'Rob', 'Walters', 0),
(5, 'EM', 'Gail', 'Erickson', 0),
(6, 'EM', 'Jossef', 'Goldberg', 0),
(7, 'SC', 'Dylan', 'Miller', 2),
(8, 'SC', 'Diane', 'Margheim', 0),
(9, 'SC', 'Gigi', 'Matthew', 0),
(10, 'IN', 'Michael', 'Raheem', 2),
(11, 'SC', 'Ovidiu', 'Cracium', 0),
(12, 'SC', 'Thierry', 'DHers', 1),
(13, 'SC', 'Janice', 'Galvin', 2),
(14, 'SC', 'Michael', 'Sullivan', 1),
(15, 'SC', 'Sharon', 'Salavaria', 1);

-- Employees
INSERT INTO HumanResources_Employee (BusinessEntityID, NationalIDNumber, LoginID, JobTitle, BirthDate, MaritalStatus, Gender, HireDate, SalariedFlag, VacationHours, SickLeaveHours, CurrentFlag) VALUES
(1, '295847284', 'adventure-works\ken0', 'Chief Executive Officer', '1969-01-29', 'S', 'M', '2009-01-14', TRUE, 99, 69, TRUE),
(2, '245797967', 'adventure-works\terri0', 'Vice President of Engineering', '1971-08-01', 'S', 'F', '2008-01-31', TRUE, 1, 20, TRUE),
(3, '509647174', 'adventure-works\roberto0', 'Engineering Manager', '1974-11-12', 'M', 'M', '2007-11-11', TRUE, 2, 21, TRUE),
(4, '112457891', 'adventure-works\rob0', 'Senior Tool Designer', '1974-12-23', 'S', 'M', '2007-12-05', FALSE, 48, 80, TRUE),
(5, '695256908', 'adventure-works\gail0', 'Design Engineer', '1952-09-27', 'M', 'F', '2008-01-06', TRUE, 5, 22, TRUE),
(6, '998320692', 'adventure-works\jossef0', 'Design Engineer', '1959-03-11', 'M', 'M', '2008-01-24', TRUE, 6, 23, TRUE),
(7, '134969118', 'adventure-works\dylan0', 'Research and Development Manager', '1987-02-24', 'M', 'M', '2009-02-08', TRUE, 61, 51, TRUE),
(8, '811994146', 'adventure-works\diane1', 'Research and Development Engineer', '1986-06-05', 'S', 'F', '2008-12-29', TRUE, 62, 52, TRUE),
(9, '658797903', 'adventure-works\gigi0', 'Research and Development Engineer', '1979-01-21', 'M', 'F', '2009-01-16', TRUE, 63, 53, TRUE),
(10, '879342154', 'adventure-works\michael6', 'Research and Development Manager', '1984-11-30', 'M', 'M', '2008-12-22', TRUE, 64, 54, TRUE);

-- Addresses
INSERT INTO Person_Address (AddressLine1, AddressLine2, City, StateProvinceID, PostalCode) VALUES
('1970 Napa Ct.', NULL, 'Bothell', 1, '98011'),
('9833 Mt. Dias Blv.', NULL, 'Bothell', 1, '98011'),
('7484 Roundtree Drive', NULL, 'Bothell', 1, '98011'),
('9539 Glenside Dr', NULL, 'Bothell', 1, '98011'),
('1226 Shoe St.', NULL, 'Bothell', 1, '98011'),
('1399 Firestone Drive', NULL, 'Bothell', 1, '98011'),
('5672 Hale Dr.', NULL, 'Bothell', 1, '98011'),
('6387 Scenic Avenue', NULL, 'Bothell', 1, '98011'),
('8713 Yosemite Ct.', NULL, 'Bothell', 1, '98011'),
('250 Race Court', NULL, 'Bothell', 1, '98011'),
('1318 Lasalle Street', NULL, 'Bothell', 1, '98011'),
('5415 San Gabriel Dr.', NULL, 'Bothell', 1, '98011');

-- Products
INSERT INTO Production_Product (Name, ProductNumber, MakeFlag, FinishedGoodsFlag, Color, SafetyStockLevel, ReorderPoint, StandardCost, ListPrice, Size, SizeUnitMeasureCode, WeightUnitMeasureCode, Weight, DaysToManufacture, ProductLine, Class, Style, ProductSubcategoryID, ProductModelID, SellStartDate) VALUES
('Adjustable Race', 'AR-5381', FALSE, FALSE, NULL, 1000, 750, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, '2008-04-30'),
('Bearing Ball', 'BA-8327', FALSE, FALSE, NULL, 1000, 750, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, '2008-04-30'),
('BB Ball Bearing', 'BE-2349', TRUE, FALSE, NULL, 800, 600, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, '2008-04-30'),
('Headset Ball Bearings', 'BE-2908', FALSE, FALSE, NULL, 800, 600, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, '2008-04-30'),
('Blade', 'BL-2036', TRUE, FALSE, NULL, 800, 600, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, '2008-04-30'),
('LL Crankarm', 'CA-5965', FALSE, FALSE, 'Black', 500, 375, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 0, 'L', 'L', NULL, NULL, NULL, '2008-04-30'),
('ML Crankarm', 'CA-6738', FALSE, FALSE, 'Black', 500, 375, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 0, 'M', 'M', NULL, NULL, NULL, '2008-04-30'),
('HL Crankarm', 'CA-7457', FALSE, FALSE, 'Black', 500, 375, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 0, 'H', 'H', NULL, NULL, NULL, '2008-04-30'),
('Chainring Bolts', 'CB-2903', FALSE, FALSE, 'Silver', 1000, 750, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, '2008-04-30'),
('Chainring Nut', 'CN-6137', FALSE, FALSE, 'Silver', 1000, 750, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, '2008-04-30'),
('Chainring', 'CR-7833', FALSE, FALSE, 'Black', 1000, 750, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, '2008-04-30'),
('Crown Race', 'CR-9981', FALSE, FALSE, 'Black', 1000, 750, 0.0000, 0.0000, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, '2008-04-30');

-- Product Inventory
INSERT INTO Production_ProductInventory (ProductID, LocationID, Shelf, Bin, Quantity) VALUES
(1, 1, 'A', 1, 408),
(1, 6, 'B', 5, 324),
(1, 50, 'A', 5, 353),
(2, 1, 'A', 2, 427),
(2, 6, 'B', 1, 318),
(2, 50, 'A', 6, 364),
(3, 1, 'A', 7, 585),
(3, 6, 'B', 6, 443),
(3, 50, 'A', 7, 324),
(4, 1, 'C', 2, 512),
(4, 6, 'B', 3, 422),
(4, 50, 'B', 1, 388);

-- Special Offer Products
INSERT INTO Sales_SpecialOfferProduct (SpecialOfferID, ProductID) VALUES
(1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (1, 7), (1, 8), (1, 9), (1, 10), (1, 11), (1, 12);

-- Sales People
INSERT INTO Sales_SalesPerson (BusinessEntityID, TerritoryID, SalesQuota, Bonus, CommissionPct, SalesYTD, SalesLastYear) VALUES
(1, 1, 300000.00, 0.00, 0.012, 559697.5639, 0.00),
(2, 4, 300000.00, 4100.00, 0.015, 3763178.1787, 1750406.4785),
(3, 3, 300000.00, 2000.00, 0.015, 4251368.5497, 1439156.0291),
(4, 6, 300000.00, 2500.00, 0.019, 3189418.3662, 1997186.2037),
(5, 5, 300000.00, 6700.00, 0.016, 1453719.4653, 896218.16);

-- Customers
INSERT INTO Sales_Customer (PersonID, StoreID, TerritoryID, AccountNumber) VALUES
(7, NULL, 1, 'AW00000001'),
(8, NULL, 1, 'AW00000002'),
(9, NULL, 1, 'AW00000003'),
(10, NULL, 1, 'AW00000004'),
(11, NULL, 1, 'AW00000005'),
(12, NULL, 1, 'AW00000006'),
(13, NULL, 1, 'AW00000007'),
(14, NULL, 1, 'AW00000008'),
(15, NULL, 1, 'AW00000009'),
(NULL, 16, 1, 'AW00000010');

-- Stores
INSERT INTO Sales_Store (BusinessEntityID, Name, SalesPersonID) VALUES
(16, 'Next-Door Bike Store', 1),
(17, 'Professional Sales and Service', 2),
(18, 'Riders Company', 3),
(19, 'The Bike Mechanics', 4),
(20, 'Satisfying Cycles', 5);

-- Sales Orders
INSERT INTO Sales_SalesOrderHeader (RevisionNumber, OrderDate, DueDate, ShipDate, Status, OnlineOrderFlag, SalesOrderNumber, CustomerID, SalesPersonID, TerritoryID, BillToAddressID, ShipToAddressID, ShipMethodID, SubTotal, TaxAmt, Freight, TotalDue) VALUES
(8, '2011-05-31', '2011-06-12', '2011-06-07', 5, FALSE, 'SO43659', 1, 1, 1, 1, 1, 5, 20565.6206, 1971.5149, 616.0984, 23153.2339),
(8, '2011-05-31', '2011-06-12', '2011-06-07', 5, FALSE, 'SO43660', 2, 1, 1, 2, 2, 5, 1294.2529, 124.2483, 38.8276, 1457.3288),
(2, '2011-05-31', '2011-06-12', '2011-06-07', 5, FALSE, 'SO43661', 3, 2, 4, 3, 3, 5, 32726.4786, 3153.7696, 985.553, 36865.8012),
(2, '2011-05-31', '2011-06-12', '2011-06-07', 5, FALSE, 'SO43662', 4, 2, 4, 4, 4, 5, 28832.5289, 2775.1646, 867.2389, 32474.9324),
(2, '2011-05-31', '2011-06-12', '2011-06-07', 5, FALSE, 'SO43663', 5, 3, 3, 5, 5, 5, 419.4589, 40.2681, 12.5838, 472.3108),
(2, '2011-05-31', '2011-06-12', '2011-06-07', 5, FALSE, 'SO43664', 6, 3, 3, 6, 6, 5, 24432.6088, 2344.9921, 732.81, 27510.4109),
(2, '2011-05-31', '2011-06-12', '2011-06-07', 5, FALSE, 'SO43665', 7, 4, 6, 7, 7, 5, 14352.7713, 1375.9427, 429.9821, 16158.6961),
(2, '2011-05-31', '2011-06-12', '2011-06-07', 5, FALSE, 'SO43666', 8, 4, 6, 8, 8, 5, 631.0681, 60.6182, 18.9427, 710.6290),
(2, '2011-05-31', '2011-06-12', '2011-06-07', 5, FALSE, 'SO43667', 9, 5, 5, 9, 9, 5, 20009.5034, 1917.0589, 599.0589, 22525.6212),
(2, '2011-05-31', '2011-06-12', '2011-06-07', 5, FALSE, 'SO43668', 10, 5, 5, 10, 10, 5, 5694.8564, 546.5851, 170.8081, 6412.2496);

-- Sales Order Details
INSERT INTO Sales_SalesOrderDetail (SalesOrderID, CarrierTrackingNumber, OrderQty, ProductID, SpecialOfferID, UnitPrice, UnitPriceDiscount, LineTotal) VALUES
(1, '4911-403C-98', 1, 1, 1, 2024.994, 0.00, 2024.994),
(1, '4911-403C-98', 3, 2, 1, 2024.994, 0.00, 6074.982),
(2, '4911-403C-98', 1, 3, 1, 2024.994, 0.00, 2024.994),
(2, '4911-403C-98', 1, 4, 1, 2024.994, 0.00, 2024.994),
(3, '4911-403C-98', 2, 5, 1, 2024.994, 0.00, 4049.988),
(3, '4911-403C-98', 1, 6, 1, 2024.994, 0.00, 2024.994),
(4, '4911-403C-98', 1, 7, 1, 2024.994, 0.00, 2024.994),
(4, '4911-403C-98', 3, 8, 1, 2024.994, 0.00, 6074.982),
(5, '4911-403C-98', 1, 9, 1, 2024.994, 0.00, 2024.994),
(5, '4911-403C-98', 1, 10, 1, 2024.994, 0.00, 2024.994),
(6, '4911-403C-98', 2, 11, 1, 2024.994, 0.00, 4049.988),
(6, '4911-403C-98', 1, 12, 1, 2024.994, 0.00, 2024.994);

-- Vendors
INSERT INTO Purchasing_Vendor (BusinessEntityID, AccountNumber, Name, CreditRating, PreferredVendorStatus, ActiveFlag) VALUES
(16, 'AUSTRALI0001', 'Australia Bike Retailer', 1, TRUE, TRUE),
(17, 'ELECTRON0001', 'Electronic Bike Repair & Supplies', 4, FALSE, TRUE),
(18, 'GREENLA0001', 'Greenwood Athletic Company', 2, TRUE, TRUE),
(19, 'INTERNAT0001', 'International Trek Center', 3, FALSE, TRUE),
(20, 'LIGHTSP0001', 'Light Speed', 2, TRUE, TRUE);

-- Purchase Orders
INSERT INTO Purchasing_PurchaseOrderHeader (RevisionNumber, Status, EmployeeID, VendorID, ShipMethodID, OrderDate, SubTotal, TaxAmt, Freight, TotalDue) VALUES
(2, 4, 1, 16, 3, '2011-04-16', 2435.3542, 195.6283, 60.8859, 2691.8684),
(2, 4, 6, 17, 3, '2011-04-16', 2435.3542, 195.6283, 60.8859, 2691.8684),
(2, 4, 3, 18, 3, '2011-04-16', 2435.3542, 195.6283, 60.8859, 2691.8684),
(2, 4, 4, 19, 3, '2011-04-16', 2435.3542, 195.6283, 60.8859, 2691.8684),
(2, 4, 5, 20, 3, '2011-04-16', 2435.3542, 195.6283, 60.8859, 2691.8684);

-- Purchase Order Details
INSERT INTO Purchasing_PurchaseOrderDetail (PurchaseOrderID, DueDate, OrderQty, ProductID, UnitPrice, LineTotal, ReceivedQty, RejectedQty, StockedQty) VALUES
(1, '2011-04-30', 4, 1, 92.8100, 371.24, 4.00, 0.00, 4.00),
(1, '2011-04-30', 3, 2, 92.8100, 278.43, 3.00, 0.00, 3.00),
(2, '2011-04-30', 4, 3, 92.8100, 371.24, 4.00, 0.00, 4.00),
(2, '2011-04-30', 3, 4, 92.8100, 278.43, 3.00, 0.00, 3.00),
(3, '2011-04-30', 4, 5, 92.8100, 371.24, 4.00, 0.00, 4.00),
(3, '2011-04-30', 3, 6, 92.8100, 278.43, 3.00, 0.00, 3.00),
(4, '2011-04-30', 4, 7, 92.8100, 371.24, 4.00, 0.00, 4.00),
(4, '2011-04-30', 3, 8, 92.8100, 278.43, 3.00, 0.00, 3.00),
(5, '2011-04-30', 4, 9, 92.8100, 371.24, 4.00, 0.00, 4.00),
(5, '2011-04-30', 3, 10, 92.8100, 278.43, 3.00, 0.00, 3.00);

-- Work Orders
INSERT INTO Production_WorkOrder (ProductID, OrderQty, StockedQty, ScrappedQty, StartDate, EndDate, DueDate, ScrapReasonID) VALUES
(1, 8, 8, 0, '2011-06-07', '2011-06-13', '2011-06-18', NULL),
(2, 8, 8, 0, '2011-06-07', '2011-06-13', '2011-06-18', NULL),
(3, 6, 6, 0, '2011-06-07', '2011-06-13', '2011-06-18', NULL),
(4, 6, 6, 0, '2011-06-07', '2011-06-13', '2011-06-18', NULL),
(5, 4, 4, 0, '2011-06-07', '2011-06-13', '2011-06-18', NULL),
(6, 4, 4, 0, '2011-06-07', '2011-06-13', '2011-06-18', NULL),
(7, 6, 6, 0, '2011-06-07', '2011-06-13', '2011-06-18', NULL),
(8, 6, 6, 0, '2011-06-07', '2011-06-13', '2011-06-18', NULL),
(9, 8, 8, 0, '2011-06-07', '2011-06-13', '2011-06-18', NULL),
(10, 8, 8, 0, '2011-06-07', '2011-06-13', '2011-06-18', NULL);

-- Additional supporting data
INSERT INTO Person_EmailAddress (BusinessEntityID, EmailAddress) VALUES
(1, 'ken0@adventure-works.com'),
(2, 'terri0@adventure-works.com'),
(3, 'roberto0@adventure-works.com'),
(4, 'rob0@adventure-works.com'),
(5, 'gail0@adventure-works.com'),
(6, 'jossef0@adventure-works.com'),
(7, 'dylan0@adventure-works.com'),
(8, 'diane1@adventure-works.com'),
(9, 'gigi0@adventure-works.com'),
(10, 'michael6@adventure-works.com');

INSERT INTO Person_PersonPhone (BusinessEntityID, PhoneNumber, PhoneNumberTypeID) VALUES
(1, '697-555-0142', 1),
(2, '819-555-0175', 1),
(3, '212-555-0187', 1),
(4, '612-555-0100', 1),
(5, '849-555-0139', 1),
(6, '122-555-0189', 1),
(7, '181-555-0156', 1),
(8, '815-555-0138', 1),
(9, '185-555-0186', 1),
(10, '330-555-2568', 1);

INSERT INTO HumanResources_EmployeeDepartmentHistory (BusinessEntityID, DepartmentID, ShiftID, StartDate, EndDate) VALUES
(1, 16, 1, '2009-01-14', NULL),
(2, 1, 1, '2008-01-31', NULL),
(3, 1, 1, '2007-11-11', NULL),
(4, 2, 1, '2007-12-05', NULL),
(5, 1, 1, '2008-01-06', NULL),
(6, 1, 1, '2008-01-24', NULL),
(7, 6, 1, '2009-02-08', NULL),
(8, 6, 1, '2008-12-29', NULL),
(9, 6, 1, '2009-01-16', NULL),
(10, 6, 1, '2008-12-22', NULL);

INSERT INTO HumanResources_EmployeePayHistory (BusinessEntityID, RateChangeDate, Rate, PayFrequency) VALUES
(1, '2009-01-14', 125.50, 2),
(2, '2008-01-31', 63.4615, 2),
(3, '2007-11-11', 43.2692, 2),
(4, '2007-12-05', 29.8462, 1),
(5, '2008-01-06', 32.6923, 2),
(6, '2008-01-24', 32.6923, 2),
(7, '2009-02-08', 50.4808, 2),
(8, '2008-12-29', 40.8654, 2),
(9, '2009-01-16', 40.8654, 2),
(10, '2008-12-22', 50.4808, 2);

INSERT INTO Sales_CountryRegionCurrency (CountryRegionCode, CurrencyCode) VALUES
('USA', 'USD'),
('CAN', 'CAD'),
('GBR', 'GBP'),
('DEU', 'EUR'),
('FRA', 'EUR'),
('JPN', 'JPY'),
('AUS', 'AUD'),
('BRA', 'BRL'),
('CHN', 'CNY'),
('IND', 'INR');

INSERT INTO Sales_CurrencyRate (CurrencyRateDate, FromCurrencyCode, ToCurrencyCode, AverageRate, EndOfDayRate) VALUES
('2011-06-07', 'USD', 'EUR', 0.7462, 0.7462),
('2011-06-07', 'USD', 'GBP', 0.6309, 0.6309),
('2011-06-07', 'USD', 'CAD', 1.0128, 1.0128),
('2011-06-07', 'USD', 'JPY', 80.2500, 80.2500),
('2011-06-07', 'USD', 'AUD', 0.9411, 0.9411),
('2011-06-07', 'USD', 'BRL', 1.5983, 1.5983),
('2011-06-07', 'USD', 'CNY', 6.4615, 6.4615),
('2011-06-07', 'USD', 'INR', 44.8500, 44.8500),
('2011-06-07', 'USD', 'MXN', 11.7647, 11.7647),
('2011-06-08', 'USD', 'EUR', 0.7463, 0.7463);

INSERT INTO Production_Culture (CultureID, Name) VALUES
('ar', 'Arabic'),
('bg', 'Bulgarian'),
('ca', 'Catalan'),
('zh-cht', 'Chinese'),
('cs', 'Czech'),
('da', 'Danish'),
('de', 'German'),
('el', 'Greek'),
('en', 'English'),
('es', 'Spanish'),
('fi', 'Finnish'),
('fr', 'French');

INSERT INTO Production_ProductDescription (Description) VALUES
('Chromoly steel.'),
('Aluminum alloy cups; large diameter spindle.'),
('Aluminum alloy cups and a hollow axle.'),
('Aluminum alloy.'),
('Aluminum alloy blade.'),
('Black aluminum alloy.'),
('Aluminum alloy crank arm. Will not fit all bikes.'),
('Aluminum alloy crank arm for road or mountain bikes.'),
('Stainless steel.'),
('Replacement nut for the crank bolt.'),
('Aluminum alloy 48 tooth chainring for road bikes.'),
('Aluminum alloy crown race for headset.');

INSERT INTO AWBuildVersion ("Database Version", VersionDate) VALUES
('10.0.40219.1', '2011-05-31 00:00:00.000');

-- Sample error log entries
INSERT INTO ErrorLog (UserName, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage) VALUES
('adventure-works\ken0', 2, 16, 1, NULL, 1, 'Invalid column name'),
('adventure-works\terri0', 207, 16, 1, 'uspGetBillOfMaterials', 15, 'Invalid column name ProductAssemblyID'),
('adventure-works\roberto0', 8134, 16, 1, NULL, 1, 'Divide by zero error encountered'),
('adventure-works\rob0', 515, 16, 1, 'uspGetEmployeeManagers', 32, 'Cannot insert the value NULL'),
('adventure-works\gail0', 547, 16, 0, NULL, 1, 'The INSERT statement conflicted with the FOREIGN KEY constraint'),
('adventure-works\jossef0', 2627, 14, 1, NULL, 1, 'Violation of PRIMARY KEY constraint'),
('adventure-works\dylan0', 8152, 16, 1, NULL, 1, 'String or binary data would be truncated'),
('adventure-works\diane1', 102, 15, 1, NULL, 1, 'Incorrect syntax near'),
('adventure-works\gigi0', 156, 15, 1, NULL, 1, 'Incorrect syntax near the keyword'),
('adventure-works\michael6', 245, 16, 1, NULL, 1, 'Conversion failed when converting');

-- Sample database log entries
INSERT INTO DatabaseLog (PostTime, DatabaseUser, Event, Schema, Object, TSQL, XmlEvent) VALUES
('2011-06-07 09:00:00.000', 'adventure-works\ken0', 'CREATE_TABLE', 'Production', 'Product', 'CREATE TABLE Production.Product...', '<EVENT_INSTANCE><EventType>CREATE_TABLE</EventType></EVENT_INSTANCE>'),
('2011-06-07 09:15:00.000', 'adventure-works\terri0', 'ALTER_TABLE', 'Sales', 'SalesOrderHeader', 'ALTER TABLE Sales.SalesOrderHeader...', '<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType></EVENT_INSTANCE>'),
('2011-06-07 09:30:00.000', 'adventure-works\roberto0', 'CREATE_INDEX', 'Person', 'Person', 'CREATE INDEX IX_Person_LastName_FirstName...', '<EVENT_INSTANCE><EventType>CREATE_INDEX</EventType></EVENT_INSTANCE>'),
('2011-06-07 09:45:00.000', 'adventure-works\rob0', 'DROP_INDEX', 'Production', 'Product', 'DROP INDEX IX_Product_Name...', '<EVENT_INSTANCE><EventType>DROP_INDEX</EventType></EVENT_INSTANCE>'),
('2011-06-07 10:00:00.000', 'adventure-works\gail0', 'CREATE_PROCEDURE', 'dbo', 'uspGetBillOfMaterials', 'CREATE PROCEDURE dbo.uspGetBillOfMaterials...', '<EVENT_INSTANCE><EventType>CREATE_PROCEDURE</EventType></EVENT_INSTANCE>'),
('2011-06-07 10:15:00.000', 'adventure-works\jossef0', 'ALTER_PROCEDURE', 'dbo', 'uspGetEmployeeManagers', 'ALTER PROCEDURE dbo.uspGetEmployeeManagers...', '<EVENT_INSTANCE><EventType>ALTER_PROCEDURE</EventType></EVENT_INSTANCE>'),
('2011-06-07 10:30:00.000', 'adventure-works\dylan0', 'CREATE_TRIGGER', 'Production', 'ProductInventory', 'CREATE TRIGGER Production.iProductInventory...', '<EVENT_INSTANCE><EventType>CREATE_TRIGGER</EventType></EVENT_INSTANCE>'),
('2011-06-07 10:45:00.000', 'adventure-works\diane1', 'CREATE_VIEW', 'HumanResources', 'vEmployee', 'CREATE VIEW HumanResources.vEmployee...', '<EVENT_INSTANCE><EventType>CREATE_VIEW</EventType></EVENT_INSTANCE>'),
('2011-06-07 11:00:00.000', 'adventure-works\gigi0', 'CREATE_FUNCTION', 'dbo', 'ufnGetContactInformation', 'CREATE FUNCTION dbo.ufnGetContactInformation...', '<EVENT_INSTANCE><EventType>CREATE_FUNCTION</EventType></EVENT_INSTANCE>'),
('2011-06-07 11:15:00.000', 'adventure-works\michael6', 'GRANT', 'Sales', 'Customer', 'GRANT SELECT ON Sales.Customer...', '<EVENT_INSTANCE><EventType>GRANT</EventType></EVENT_INSTANCE>');

COMMIT;