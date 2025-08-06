-- AdventureWorks Snowflake Database Creation Script
-- Complete schema with all tables, columns, and relationships

CREATE OR REPLACE DATABASE ADVENTUREWORKS;
USE DATABASE ADVENTUREWORKS;
USE SCHEMA PUBLIC;

-- Person Schema Tables
CREATE OR REPLACE TABLE Person_Address (
    AddressID INTEGER AUTOINCREMENT PRIMARY KEY,
    AddressLine1 VARCHAR(60) NOT NULL,
    AddressLine2 VARCHAR(60),
    City VARCHAR(30) NOT NULL,
    StateProvinceID INTEGER NOT NULL,
    PostalCode VARCHAR(15) NOT NULL,
    SpatialLocation GEOGRAPHY,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Person_AddressType (
    AddressTypeID INTEGER AUTOINCREMENT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Person_BusinessEntity (
    BusinessEntityID INTEGER AUTOINCREMENT PRIMARY KEY,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Person_BusinessEntityAddress (
    BusinessEntityID INTEGER NOT NULL,
    AddressID INTEGER NOT NULL,
    AddressTypeID INTEGER NOT NULL,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (BusinessEntityID, AddressID, AddressTypeID)
);

CREATE OR REPLACE TABLE Person_BusinessEntityContact (
    BusinessEntityID INTEGER NOT NULL,
    PersonID INTEGER NOT NULL,
    ContactTypeID INTEGER NOT NULL,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (BusinessEntityID, PersonID, ContactTypeID)
);

CREATE OR REPLACE TABLE Person_ContactType (
    ContactTypeID INTEGER AUTOINCREMENT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Person_CountryRegion (
    CountryRegionCode VARCHAR(3) PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Person_EmailAddress (
    BusinessEntityID INTEGER NOT NULL,
    EmailAddressID INTEGER AUTOINCREMENT,
    EmailAddress VARCHAR(50),
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (BusinessEntityID, EmailAddressID)
);

CREATE OR REPLACE TABLE Person_Password (
    BusinessEntityID INTEGER NOT NULL PRIMARY KEY,
    PasswordHash VARCHAR(128) NOT NULL,
    PasswordSalt VARCHAR(10) NOT NULL,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Person_Person (
    BusinessEntityID INTEGER NOT NULL PRIMARY KEY,
    PersonType CHAR(2) NOT NULL,
    NameStyle BOOLEAN NOT NULL DEFAULT FALSE,
    Title VARCHAR(8),
    FirstName VARCHAR(50) NOT NULL,
    MiddleName VARCHAR(50),
    LastName VARCHAR(50) NOT NULL,
    Suffix VARCHAR(10),
    EmailPromotion INTEGER NOT NULL DEFAULT 0,
    AdditionalContactInfo STRING,
    Demographics STRING,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Person_PersonPhone (
    BusinessEntityID INTEGER NOT NULL,
    PhoneNumber VARCHAR(25) NOT NULL,
    PhoneNumberTypeID INTEGER NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (BusinessEntityID, PhoneNumber, PhoneNumberTypeID)
);

CREATE OR REPLACE TABLE Person_PhoneNumberType (
    PhoneNumberTypeID INTEGER AUTOINCREMENT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Person_StateProvince (
    StateProvinceID INTEGER AUTOINCREMENT PRIMARY KEY,
    StateProvinceCode CHAR(3) NOT NULL,
    CountryRegionCode VARCHAR(3) NOT NULL,
    IsOnlyStateProvinceFlag BOOLEAN NOT NULL DEFAULT TRUE,
    Name VARCHAR(50) NOT NULL,
    TerritoryID INTEGER NOT NULL,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- Sales Schema Tables
CREATE OR REPLACE TABLE Sales_CountryRegionCurrency (
    CountryRegionCode VARCHAR(3) NOT NULL,
    CurrencyCode CHAR(3) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (CountryRegionCode, CurrencyCode)
);

CREATE OR REPLACE TABLE Sales_CreditCard (
    CreditCardID INTEGER AUTOINCREMENT PRIMARY KEY,
    CardType VARCHAR(50) NOT NULL,
    CardNumber VARCHAR(25) NOT NULL,
    ExpMonth TINYINT NOT NULL,
    ExpYear SMALLINT NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Sales_Currency (
    CurrencyCode CHAR(3) PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Sales_CurrencyRate (
    CurrencyRateID INTEGER AUTOINCREMENT PRIMARY KEY,
    CurrencyRateDate TIMESTAMP_NTZ NOT NULL,
    FromCurrencyCode CHAR(3) NOT NULL,
    ToCurrencyCode CHAR(3) NOT NULL,
    AverageRate DECIMAL(19,4) NOT NULL,
    EndOfDayRate DECIMAL(19,4) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Sales_Customer (
    CustomerID INTEGER AUTOINCREMENT PRIMARY KEY,
    PersonID INTEGER,
    StoreID INTEGER,
    TerritoryID INTEGER,
    AccountNumber VARCHAR(15),
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Sales_PersonCreditCard (
    BusinessEntityID INTEGER NOT NULL,
    CreditCardID INTEGER NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (BusinessEntityID, CreditCardID)
);

CREATE OR REPLACE TABLE Sales_SalesOrderDetail (
    SalesOrderID INTEGER NOT NULL,
    SalesOrderDetailID INTEGER AUTOINCREMENT,
    CarrierTrackingNumber VARCHAR(25),
    OrderQty SMALLINT NOT NULL,
    ProductID INTEGER NOT NULL,
    SpecialOfferID INTEGER NOT NULL,
    UnitPrice DECIMAL(19,4) NOT NULL,
    UnitPriceDiscount DECIMAL(19,4) NOT NULL DEFAULT 0.0,
    LineTotal DECIMAL(19,4),
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SalesOrderID, SalesOrderDetailID)
);

CREATE OR REPLACE TABLE Sales_SalesOrderHeader (
    SalesOrderID INTEGER AUTOINCREMENT PRIMARY KEY,
    RevisionNumber TINYINT NOT NULL DEFAULT 0,
    OrderDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    DueDate TIMESTAMP_NTZ NOT NULL,
    ShipDate TIMESTAMP_NTZ,
    Status TINYINT NOT NULL DEFAULT 1,
    OnlineOrderFlag BOOLEAN NOT NULL DEFAULT TRUE,
    SalesOrderNumber VARCHAR(25),
    PurchaseOrderNumber VARCHAR(25),
    AccountNumber VARCHAR(15),
    CustomerID INTEGER NOT NULL,
    SalesPersonID INTEGER,
    TerritoryID INTEGER,
    BillToAddressID INTEGER NOT NULL,
    ShipToAddressID INTEGER NOT NULL,
    ShipMethodID INTEGER NOT NULL,
    CreditCardID INTEGER,
    CreditCardApprovalCode VARCHAR(15),
    CurrencyRateID INTEGER,
    SubTotal DECIMAL(19,4) NOT NULL DEFAULT 0.00,
    TaxAmt DECIMAL(19,4) NOT NULL DEFAULT 0.00,
    Freight DECIMAL(19,4) NOT NULL DEFAULT 0.00,
    TotalDue DECIMAL(19,4),
    Comment VARCHAR(128),
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Sales_SalesOrderHeaderSalesReason (
    SalesOrderID INTEGER NOT NULL,
    SalesReasonID INTEGER NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SalesOrderID, SalesReasonID)
);

CREATE OR REPLACE TABLE Sales_SalesPerson (
    BusinessEntityID INTEGER NOT NULL PRIMARY KEY,
    TerritoryID INTEGER,
    SalesQuota DECIMAL(19,4),
    Bonus DECIMAL(19,4) NOT NULL DEFAULT 0.00,
    CommissionPct DECIMAL(10,4) NOT NULL DEFAULT 0.00,
    SalesYTD DECIMAL(19,4) NOT NULL DEFAULT 0.00,
    SalesLastYear DECIMAL(19,4) NOT NULL DEFAULT 0.00,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Sales_SalesPersonQuotaHistory (
    BusinessEntityID INTEGER NOT NULL,
    QuotaDate TIMESTAMP_NTZ NOT NULL,
    SalesQuota DECIMAL(19,4) NOT NULL,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (BusinessEntityID, QuotaDate)
);

CREATE OR REPLACE TABLE Sales_SalesReason (
    SalesReasonID INTEGER AUTOINCREMENT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ReasonType VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Sales_SalesTaxRate (
    SalesTaxRateID INTEGER AUTOINCREMENT PRIMARY KEY,
    StateProvinceID INTEGER NOT NULL,
    TaxType TINYINT NOT NULL,
    TaxRate DECIMAL(10,4) NOT NULL DEFAULT 0.00,
    Name VARCHAR(50) NOT NULL,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Sales_SalesTerritory (
    TerritoryID INTEGER AUTOINCREMENT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    CountryRegionCode VARCHAR(3) NOT NULL,
    "Group" VARCHAR(50) NOT NULL,
    SalesYTD DECIMAL(19,4) NOT NULL DEFAULT 0.00,
    SalesLastYear DECIMAL(19,4) NOT NULL DEFAULT 0.00,
    CostYTD DECIMAL(19,4) NOT NULL DEFAULT 0.00,
    CostLastYear DECIMAL(19,4) NOT NULL DEFAULT 0.00,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Sales_SalesTerritoryHistory (
    BusinessEntityID INTEGER NOT NULL,
    TerritoryID INTEGER NOT NULL,
    StartDate TIMESTAMP_NTZ NOT NULL,
    EndDate TIMESTAMP_NTZ,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (BusinessEntityID, StartDate, TerritoryID)
);

CREATE OR REPLACE TABLE Sales_ShoppingCartItem (
    ShoppingCartItemID INTEGER AUTOINCREMENT PRIMARY KEY,
    ShoppingCartID VARCHAR(50) NOT NULL,
    Quantity INTEGER NOT NULL DEFAULT 1,
    ProductID INTEGER NOT NULL,
    DateCreated TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Sales_SpecialOffer (
    SpecialOfferID INTEGER AUTOINCREMENT PRIMARY KEY,
    Description VARCHAR(255) NOT NULL,
    DiscountPct DECIMAL(10,4) NOT NULL DEFAULT 0.00,
    Type VARCHAR(50) NOT NULL,
    Category VARCHAR(50) NOT NULL,
    StartDate TIMESTAMP_NTZ NOT NULL,
    EndDate TIMESTAMP_NTZ NOT NULL,
    MinQty INTEGER NOT NULL DEFAULT 0,
    MaxQty INTEGER,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Sales_SpecialOfferProduct (
    SpecialOfferID INTEGER NOT NULL,
    ProductID INTEGER NOT NULL,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SpecialOfferID, ProductID)
);

CREATE OR REPLACE TABLE Sales_Store (
    BusinessEntityID INTEGER NOT NULL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    SalesPersonID INTEGER,
    Demographics STRING,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- Production Schema Tables
CREATE OR REPLACE TABLE Production_BillOfMaterials (
    BillOfMaterialsID INTEGER AUTOINCREMENT PRIMARY KEY,
    ProductAssemblyID INTEGER,
    ComponentID INTEGER NOT NULL,
    StartDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    EndDate TIMESTAMP_NTZ,
    UnitMeasureCode CHAR(3) NOT NULL,
    BOMLevel SMALLINT NOT NULL,
    PerAssemblyQty DECIMAL(8,2) NOT NULL DEFAULT 1.00,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_Culture (
    CultureID CHAR(6) PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_Document (
    DocumentNode STRING NOT NULL PRIMARY KEY,
    DocumentLevel INTEGER,
    Title VARCHAR(50) NOT NULL,
    Owner INTEGER NOT NULL,
    FolderFlag BOOLEAN NOT NULL DEFAULT FALSE,
    FileName VARCHAR(400) NOT NULL,
    FileExtension VARCHAR(8) NOT NULL,
    Revision CHAR(5) NOT NULL,
    ChangeNumber INTEGER NOT NULL DEFAULT 0,
    Status TINYINT NOT NULL,
    DocumentSummary STRING,
    Document BINARY,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_Illustration (
    IllustrationID INTEGER AUTOINCREMENT PRIMARY KEY,
    Diagram STRING,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_Location (
    LocationID SMALLINT AUTOINCREMENT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    CostRate DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    Availability DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_Product (
    ProductID INTEGER AUTOINCREMENT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ProductNumber VARCHAR(25) NOT NULL,
    MakeFlag BOOLEAN NOT NULL DEFAULT TRUE,
    FinishedGoodsFlag BOOLEAN NOT NULL DEFAULT TRUE,
    Color VARCHAR(15),
    SafetyStockLevel SMALLINT NOT NULL,
    ReorderPoint SMALLINT NOT NULL,
    StandardCost DECIMAL(19,4) NOT NULL,
    ListPrice DECIMAL(19,4) NOT NULL,
    Size VARCHAR(5),
    SizeUnitMeasureCode CHAR(3),
    WeightUnitMeasureCode CHAR(3),
    Weight DECIMAL(8,2),
    DaysToManufacture INTEGER NOT NULL,
    ProductLine CHAR(2),
    Class CHAR(2),
    Style CHAR(2),
    ProductSubcategoryID INTEGER,
    ProductModelID INTEGER,
    SellStartDate TIMESTAMP_NTZ NOT NULL,
    SellEndDate TIMESTAMP_NTZ,
    DiscontinuedDate TIMESTAMP_NTZ,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_ProductCategory (
    ProductCategoryID INTEGER AUTOINCREMENT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_ProductCostHistory (
    ProductID INTEGER NOT NULL,
    StartDate TIMESTAMP_NTZ NOT NULL,
    EndDate TIMESTAMP_NTZ,
    StandardCost DECIMAL(19,4) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ProductID, StartDate)
);

CREATE OR REPLACE TABLE Production_ProductDescription (
    ProductDescriptionID INTEGER AUTOINCREMENT PRIMARY KEY,
    Description VARCHAR(400) NOT NULL,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_ProductDocument (
    ProductID INTEGER NOT NULL,
    DocumentNode STRING NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ProductID, DocumentNode)
);

CREATE OR REPLACE TABLE Production_ProductInventory (
    ProductID INTEGER NOT NULL,
    LocationID SMALLINT NOT NULL,
    Shelf VARCHAR(10) NOT NULL,
    Bin TINYINT NOT NULL,
    Quantity SMALLINT NOT NULL DEFAULT 0,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ProductID, LocationID)
);

CREATE OR REPLACE TABLE Production_ProductListPriceHistory (
    ProductID INTEGER NOT NULL,
    StartDate TIMESTAMP_NTZ NOT NULL,
    EndDate TIMESTAMP_NTZ,
    ListPrice DECIMAL(19,4) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ProductID, StartDate)
);

CREATE OR REPLACE TABLE Production_ProductModel (
    ProductModelID INTEGER AUTOINCREMENT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    CatalogDescription STRING,
    Instructions STRING,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_ProductModelIllustration (
    ProductModelID INTEGER NOT NULL,
    IllustrationID INTEGER NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ProductModelID, IllustrationID)
);

CREATE OR REPLACE TABLE Production_ProductModelProductDescriptionCulture (
    ProductModelID INTEGER NOT NULL,
    ProductDescriptionID INTEGER NOT NULL,
    CultureID CHAR(6) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ProductModelID, ProductDescriptionID, CultureID)
);

CREATE OR REPLACE TABLE Production_ProductPhoto (
    ProductPhotoID INTEGER AUTOINCREMENT PRIMARY KEY,
    ThumbNailPhoto BINARY,
    ThumbnailPhotoFileName VARCHAR(50),
    LargePhoto BINARY,
    LargePhotoFileName VARCHAR(50),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_ProductProductPhoto (
    ProductID INTEGER NOT NULL,
    ProductPhotoID INTEGER NOT NULL,
    Primary BOOLEAN NOT NULL DEFAULT FALSE,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ProductID, ProductPhotoID)
);

CREATE OR REPLACE TABLE Production_ProductReview (
    ProductReviewID INTEGER AUTOINCREMENT PRIMARY KEY,
    ProductID INTEGER NOT NULL,
    ReviewerName VARCHAR(50) NOT NULL,
    ReviewDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    EmailAddress VARCHAR(50) NOT NULL,
    Rating INTEGER NOT NULL,
    Comments VARCHAR(3850),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_ProductSubcategory (
    ProductSubcategoryID INTEGER AUTOINCREMENT PRIMARY KEY,
    ProductCategoryID INTEGER NOT NULL,
    Name VARCHAR(50) NOT NULL,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_ScrapReason (
    ScrapReasonID SMALLINT AUTOINCREMENT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_TransactionHistory (
    TransactionID INTEGER AUTOINCREMENT PRIMARY KEY,
    ProductID INTEGER NOT NULL,
    ReferenceOrderID INTEGER NOT NULL,
    ReferenceOrderLineID INTEGER NOT NULL DEFAULT 0,
    TransactionDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    TransactionType CHAR(1) NOT NULL,
    Quantity INTEGER NOT NULL,
    ActualCost DECIMAL(19,4) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_TransactionHistoryArchive (
    TransactionID INTEGER NOT NULL PRIMARY KEY,
    ProductID INTEGER NOT NULL,
    ReferenceOrderID INTEGER NOT NULL,
    ReferenceOrderLineID INTEGER NOT NULL DEFAULT 0,
    TransactionDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    TransactionType CHAR(1) NOT NULL,
    Quantity INTEGER NOT NULL,
    ActualCost DECIMAL(19,4) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_UnitMeasure (
    UnitMeasureCode CHAR(3) PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_WorkOrder (
    WorkOrderID INTEGER AUTOINCREMENT PRIMARY KEY,
    ProductID INTEGER NOT NULL,
    OrderQty INTEGER NOT NULL,
    StockedQty INTEGER,
    ScrappedQty SMALLINT NOT NULL,
    StartDate TIMESTAMP_NTZ NOT NULL,
    EndDate TIMESTAMP_NTZ,
    DueDate TIMESTAMP_NTZ NOT NULL,
    ScrapReasonID SMALLINT,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Production_WorkOrderRouting (
    WorkOrderID INTEGER NOT NULL,
    ProductID INTEGER NOT NULL,
    OperationSequence SMALLINT NOT NULL,
    LocationID SMALLINT NOT NULL,
    ScheduledStartDate TIMESTAMP_NTZ NOT NULL,
    ScheduledEndDate TIMESTAMP_NTZ NOT NULL,
    ActualStartDate TIMESTAMP_NTZ,
    ActualEndDate TIMESTAMP_NTZ,
    ActualResourceHrs DECIMAL(9,4),
    PlannedCost DECIMAL(19,4) NOT NULL,
    ActualCost DECIMAL(19,4),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (WorkOrderID, ProductID, OperationSequence)
);

-- Purchasing Schema Tables
CREATE OR REPLACE TABLE Purchasing_ProductVendor (
    ProductID INTEGER NOT NULL,
    BusinessEntityID INTEGER NOT NULL,
    AverageLeadTime INTEGER NOT NULL,
    StandardPrice DECIMAL(19,4) NOT NULL,
    LastReceiptCost DECIMAL(19,4),
    LastReceiptDate TIMESTAMP_NTZ,
    MinOrderQty INTEGER NOT NULL,
    MaxOrderQty INTEGER NOT NULL,
    OnOrderQty INTEGER,
    UnitMeasureCode CHAR(3) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ProductID, BusinessEntityID)
);

CREATE OR REPLACE TABLE Purchasing_PurchaseOrderDetail (
    PurchaseOrderID INTEGER NOT NULL,
    PurchaseOrderDetailID INTEGER AUTOINCREMENT,
    DueDate TIMESTAMP_NTZ NOT NULL,
    OrderQty SMALLINT NOT NULL,
    ProductID INTEGER NOT NULL,
    UnitPrice DECIMAL(19,4) NOT NULL,
    LineTotal DECIMAL(19,4),
    ReceivedQty DECIMAL(8,2) NOT NULL,
    RejectedQty DECIMAL(8,2) NOT NULL,
    StockedQty DECIMAL(8,2),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (PurchaseOrderID, PurchaseOrderDetailID)
);

CREATE OR REPLACE TABLE Purchasing_PurchaseOrderHeader (
    PurchaseOrderID INTEGER AUTOINCREMENT PRIMARY KEY,
    RevisionNumber TINYINT NOT NULL DEFAULT 0,
    Status TINYINT NOT NULL DEFAULT 1,
    EmployeeID INTEGER NOT NULL,
    VendorID INTEGER NOT NULL,
    ShipMethodID INTEGER NOT NULL,
    OrderDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    ShipDate TIMESTAMP_NTZ,
    SubTotal DECIMAL(19,4) NOT NULL DEFAULT 0.00,
    TaxAmt DECIMAL(19,4) NOT NULL DEFAULT 0.00,
    Freight DECIMAL(19,4) NOT NULL DEFAULT 0.00,
    TotalDue DECIMAL(19,4),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Purchasing_ShipMethod (
    ShipMethodID INTEGER AUTOINCREMENT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    ShipBase DECIMAL(19,4) NOT NULL DEFAULT 0.00,
    ShipRate DECIMAL(19,4) NOT NULL DEFAULT 0.00,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE Purchasing_Vendor (
    BusinessEntityID INTEGER NOT NULL PRIMARY KEY,
    AccountNumber VARCHAR(15) NOT NULL,
    Name VARCHAR(50) NOT NULL,
    CreditRating TINYINT NOT NULL,
    PreferredVendorStatus BOOLEAN NOT NULL DEFAULT TRUE,
    ActiveFlag BOOLEAN NOT NULL DEFAULT TRUE,
    PurchasingWebServiceURL VARCHAR(1024),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- HumanResources Schema Tables
CREATE OR REPLACE TABLE HumanResources_Department (
    DepartmentID SMALLINT AUTOINCREMENT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    GroupName VARCHAR(50) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE HumanResources_Employee (
    BusinessEntityID INTEGER NOT NULL PRIMARY KEY,
    NationalIDNumber VARCHAR(15) NOT NULL,
    LoginID VARCHAR(256) NOT NULL,
    OrganizationNode STRING,
    OrganizationLevel INTEGER,
    JobTitle VARCHAR(50) NOT NULL,
    BirthDate DATE NOT NULL,
    MaritalStatus CHAR(1) NOT NULL,
    Gender CHAR(1) NOT NULL,
    HireDate DATE NOT NULL,
    SalariedFlag BOOLEAN NOT NULL DEFAULT TRUE,
    VacationHours SMALLINT NOT NULL DEFAULT 0,
    SickLeaveHours SMALLINT NOT NULL DEFAULT 0,
    CurrentFlag BOOLEAN NOT NULL DEFAULT TRUE,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE HumanResources_EmployeeDepartmentHistory (
    BusinessEntityID INTEGER NOT NULL,
    DepartmentID SMALLINT NOT NULL,
    ShiftID TINYINT NOT NULL,
    StartDate DATE NOT NULL,
    EndDate DATE,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (BusinessEntityID, StartDate, DepartmentID, ShiftID)
);

CREATE OR REPLACE TABLE HumanResources_EmployeePayHistory (
    BusinessEntityID INTEGER NOT NULL,
    RateChangeDate TIMESTAMP_NTZ NOT NULL,
    Rate DECIMAL(19,4) NOT NULL,
    PayFrequency TINYINT NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (BusinessEntityID, RateChangeDate)
);

CREATE OR REPLACE TABLE HumanResources_JobCandidate (
    JobCandidateID INTEGER AUTOINCREMENT PRIMARY KEY,
    BusinessEntityID INTEGER,
    Resume STRING,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE HumanResources_Shift (
    ShiftID TINYINT AUTOINCREMENT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    StartTime TIME NOT NULL,
    EndTime TIME NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- System Tables
CREATE OR REPLACE TABLE AWBuildVersion (
    SystemInformationID SMALLINT AUTOINCREMENT PRIMARY KEY,
    "Database Version" VARCHAR(25) NOT NULL,
    VersionDate TIMESTAMP_NTZ NOT NULL,
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE DatabaseLog (
    DatabaseLogID INTEGER AUTOINCREMENT PRIMARY KEY,
    PostTime TIMESTAMP_NTZ NOT NULL,
    DatabaseUser VARCHAR(255) NOT NULL,
    Event VARCHAR(255) NOT NULL,
    Schema VARCHAR(255),
    Object VARCHAR(255),
    TSQL STRING NOT NULL,
    XmlEvent STRING NOT NULL
);

CREATE OR REPLACE TABLE ErrorLog (
    ErrorLogID INTEGER AUTOINCREMENT PRIMARY KEY,
    ErrorTime TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    UserName VARCHAR(255) NOT NULL,
    ErrorNumber INTEGER NOT NULL,
    ErrorSeverity INTEGER,
    ErrorState INTEGER,
    ErrorProcedure VARCHAR(126),
    ErrorLine INTEGER,
    ErrorMessage VARCHAR(4000) NOT NULL
);

-- Add Foreign Key Constraints
ALTER TABLE Person_Address ADD CONSTRAINT FK_Address_StateProvince 
    FOREIGN KEY (StateProvinceID) REFERENCES Person_StateProvince(StateProvinceID);

ALTER TABLE Person_BusinessEntityAddress ADD CONSTRAINT FK_BusinessEntityAddress_Address 
    FOREIGN KEY (AddressID) REFERENCES Person_Address(AddressID);

ALTER TABLE Person_BusinessEntityAddress ADD CONSTRAINT FK_BusinessEntityAddress_AddressType 
    FOREIGN KEY (AddressTypeID) REFERENCES Person_AddressType(AddressTypeID);

ALTER TABLE Person_BusinessEntityAddress ADD CONSTRAINT FK_BusinessEntityAddress_BusinessEntity 
    FOREIGN KEY (BusinessEntityID) REFERENCES Person_BusinessEntity(BusinessEntityID);

ALTER TABLE Person_BusinessEntityContact ADD CONSTRAINT FK_BusinessEntityContact_Person 
    FOREIGN KEY (PersonID) REFERENCES Person_Person(BusinessEntityID);

ALTER TABLE Person_BusinessEntityContact ADD CONSTRAINT FK_BusinessEntityContact_ContactType 
    FOREIGN KEY (ContactTypeID) REFERENCES Person_ContactType(ContactTypeID);

ALTER TABLE Person_BusinessEntityContact ADD CONSTRAINT FK_BusinessEntityContact_BusinessEntity 
    FOREIGN KEY (BusinessEntityID) REFERENCES Person_BusinessEntity(BusinessEntityID);

ALTER TABLE Person_EmailAddress ADD CONSTRAINT FK_EmailAddress_Person 
    FOREIGN KEY (BusinessEntityID) REFERENCES Person_Person(BusinessEntityID);

ALTER TABLE Person_Password ADD CONSTRAINT FK_Password_Person 
    FOREIGN KEY (BusinessEntityID) REFERENCES Person_Person(BusinessEntityID);

ALTER TABLE Person_Person ADD CONSTRAINT FK_Person_BusinessEntity 
    FOREIGN KEY (BusinessEntityID) REFERENCES Person_BusinessEntity(BusinessEntityID);

ALTER TABLE Person_PersonPhone ADD CONSTRAINT FK_PersonPhone_Person 
    FOREIGN KEY (BusinessEntityID) REFERENCES Person_Person(BusinessEntityID);

ALTER TABLE Person_PersonPhone ADD CONSTRAINT FK_PersonPhone_PhoneNumberType 
    FOREIGN KEY (PhoneNumberTypeID) REFERENCES Person_PhoneNumberType(PhoneNumberTypeID);

ALTER TABLE Person_StateProvince ADD CONSTRAINT FK_StateProvince_CountryRegion 
    FOREIGN KEY (CountryRegionCode) REFERENCES Person_CountryRegion(CountryRegionCode);

ALTER TABLE Person_StateProvince ADD CONSTRAINT FK_StateProvince_SalesTerritory 
    FOREIGN KEY (TerritoryID) REFERENCES Sales_SalesTerritory(TerritoryID);

ALTER TABLE Sales_CountryRegionCurrency ADD CONSTRAINT FK_CountryRegionCurrency_CountryRegion 
    FOREIGN KEY (CountryRegionCode) REFERENCES Person_CountryRegion(CountryRegionCode);

ALTER TABLE Sales_CountryRegionCurrency ADD CONSTRAINT FK_CountryRegionCurrency_Currency 
    FOREIGN KEY (CurrencyCode) REFERENCES Sales_Currency(CurrencyCode);

ALTER TABLE Sales_CurrencyRate ADD CONSTRAINT FK_CurrencyRate_Currency_FromCurrencyCode 
    FOREIGN KEY (FromCurrencyCode) REFERENCES Sales_Currency(CurrencyCode);

ALTER TABLE Sales_CurrencyRate ADD CONSTRAINT FK_CurrencyRate_Currency_ToCurrencyCode 
    FOREIGN KEY (ToCurrencyCode) REFERENCES Sales_Currency(CurrencyCode);

ALTER TABLE Sales_Customer ADD CONSTRAINT FK_Customer_Person 
    FOREIGN KEY (PersonID) REFERENCES Person_Person(BusinessEntityID);

ALTER TABLE Sales_Customer ADD CONSTRAINT FK_Customer_Store 
    FOREIGN KEY (StoreID) REFERENCES Sales_Store(BusinessEntityID);

ALTER TABLE Sales_Customer ADD CONSTRAINT FK_Customer_SalesTerritory 
    FOREIGN KEY (TerritoryID) REFERENCES Sales_SalesTerritory(TerritoryID);

ALTER TABLE Sales_PersonCreditCard ADD CONSTRAINT FK_PersonCreditCard_Person 
    FOREIGN KEY (BusinessEntityID) REFERENCES Person_Person(BusinessEntityID);

ALTER TABLE Sales_PersonCreditCard ADD CONSTRAINT FK_PersonCreditCard_CreditCard 
    FOREIGN KEY (CreditCardID) REFERENCES Sales_CreditCard(CreditCardID);

ALTER TABLE Sales_SalesOrderDetail ADD CONSTRAINT FK_SalesOrderDetail_SalesOrderHeader 
    FOREIGN KEY (SalesOrderID) REFERENCES Sales_SalesOrderHeader(SalesOrderID);

ALTER TABLE Sales_SalesOrderDetail ADD CONSTRAINT FK_SalesOrderDetail_SpecialOfferProduct 
    FOREIGN KEY (SpecialOfferID, ProductID) REFERENCES Sales_SpecialOfferProduct(SpecialOfferID, ProductID);

ALTER TABLE Sales_SalesOrderHeader ADD CONSTRAINT FK_SalesOrderHeader_Customer 
    FOREIGN KEY (CustomerID) REFERENCES Sales_Customer(CustomerID);

ALTER TABLE Sales_SalesOrderHeader ADD CONSTRAINT FK_SalesOrderHeader_SalesPerson 
    FOREIGN KEY (SalesPersonID) REFERENCES Sales_SalesPerson(BusinessEntityID);

ALTER TABLE Sales_SalesOrderHeader ADD CONSTRAINT FK_SalesOrderHeader_SalesTerritory 
    FOREIGN KEY (TerritoryID) REFERENCES Sales_SalesTerritory(TerritoryID);

ALTER TABLE Sales_SalesOrderHeader ADD CONSTRAINT FK_SalesOrderHeader_Address_BillToAddressID 
    FOREIGN KEY (BillToAddressID) REFERENCES Person_Address(AddressID);

ALTER TABLE Sales_SalesOrderHeader ADD CONSTRAINT FK_SalesOrderHeader_Address_ShipToAddressID 
    FOREIGN KEY (ShipToAddressID) REFERENCES Person_Address(AddressID);

ALTER TABLE Sales_SalesOrderHeader ADD CONSTRAINT FK_SalesOrderHeader_ShipMethod 
    FOREIGN KEY (ShipMethodID) REFERENCES Purchasing_ShipMethod(ShipMethodID);

ALTER TABLE Sales_SalesOrderHeader ADD CONSTRAINT FK_SalesOrderHeader_CreditCard 
    FOREIGN KEY (CreditCardID) REFERENCES Sales_CreditCard(CreditCardID);

ALTER TABLE Sales_SalesOrderHeader ADD CONSTRAINT FK_SalesOrderHeader_CurrencyRate 
    FOREIGN KEY (CurrencyRateID) REFERENCES Sales_CurrencyRate(CurrencyRateID);

ALTER TABLE Sales_SalesOrderHeaderSalesReason ADD CONSTRAINT FK_SalesOrderHeaderSalesReason_SalesOrderHeader 
    FOREIGN KEY (SalesOrderID) REFERENCES Sales_SalesOrderHeader(SalesOrderID);

ALTER TABLE Sales_SalesOrderHeaderSalesReason ADD CONSTRAINT FK_SalesOrderHeaderSalesReason_SalesReason 
    FOREIGN KEY (SalesReasonID) REFERENCES Sales_SalesReason(SalesReasonID);

ALTER TABLE Sales_SalesPerson ADD CONSTRAINT FK_SalesPerson_Employee 
    FOREIGN KEY (BusinessEntityID) REFERENCES HumanResources_Employee(BusinessEntityID);

ALTER TABLE Sales_SalesPerson ADD CONSTRAINT FK_SalesPerson_SalesTerritory 
    FOREIGN KEY (TerritoryID) REFERENCES Sales_SalesTerritory(TerritoryID);

ALTER TABLE Sales_SalesPersonQuotaHistory ADD CONSTRAINT FK_SalesPersonQuotaHistory_SalesPerson 
    FOREIGN KEY (BusinessEntityID) REFERENCES Sales_SalesPerson(BusinessEntityID);

ALTER TABLE Sales_SalesTaxRate ADD CONSTRAINT FK_SalesTaxRate_StateProvince 
    FOREIGN KEY (StateProvinceID) REFERENCES Person_StateProvince(StateProvinceID);

ALTER TABLE Sales_SalesTerritory ADD CONSTRAINT FK_SalesTerritory_CountryRegion 
    FOREIGN KEY (CountryRegionCode) REFERENCES Person_CountryRegion(CountryRegionCode);

ALTER TABLE Sales_SalesTerritoryHistory ADD CONSTRAINT FK_SalesTerritoryHistory_SalesPerson 
    FOREIGN KEY (BusinessEntityID) REFERENCES Sales_SalesPerson(BusinessEntityID);

ALTER TABLE Sales_SalesTerritoryHistory ADD CONSTRAINT FK_SalesTerritoryHistory_SalesTerritory 
    FOREIGN KEY (TerritoryID) REFERENCES Sales_SalesTerritory(TerritoryID);

ALTER TABLE Sales_ShoppingCartItem ADD CONSTRAINT FK_ShoppingCartItem_Product 
    FOREIGN KEY (ProductID) REFERENCES Production_Product(ProductID);

ALTER TABLE Sales_SpecialOfferProduct ADD CONSTRAINT FK_SpecialOfferProduct_Product 
    FOREIGN KEY (ProductID) REFERENCES Production_Product(ProductID);

ALTER TABLE Sales_SpecialOfferProduct ADD CONSTRAINT FK_SpecialOfferProduct_SpecialOffer 
    FOREIGN KEY (SpecialOfferID) REFERENCES Sales_SpecialOffer(SpecialOfferID);

ALTER TABLE Sales_Store ADD CONSTRAINT FK_Store_BusinessEntity 
    FOREIGN KEY (BusinessEntityID) REFERENCES Person_BusinessEntity(BusinessEntityID);

ALTER TABLE Sales_Store ADD CONSTRAINT FK_Store_SalesPerson 
    FOREIGN KEY (SalesPersonID) REFERENCES Sales_SalesPerson(BusinessEntityID);

ALTER TABLE Production_BillOfMaterials ADD CONSTRAINT FK_BillOfMaterials_Product_ProductAssemblyID 
    FOREIGN KEY (ProductAssemblyID) REFERENCES Production_Product(ProductID);

ALTER TABLE Production_BillOfMaterials ADD CONSTRAINT FK_BillOfMaterials_Product_ComponentID 
    FOREIGN KEY (ComponentID) REFERENCES Production_Product(ProductID);

ALTER TABLE Production_BillOfMaterials ADD CONSTRAINT FK_BillOfMaterials_UnitMeasure 
    FOREIGN KEY (UnitMeasureCode) REFERENCES Production_UnitMeasure(UnitMeasureCode);

ALTER TABLE Production_Document ADD CONSTRAINT FK_Document_Employee_Owner 
    FOREIGN KEY (Owner) REFERENCES HumanResources_Employee(BusinessEntityID);

ALTER TABLE Production_Product ADD CONSTRAINT FK_Product_ProductModel 
    FOREIGN KEY (ProductModelID) REFERENCES Production_ProductModel(ProductModelID);

ALTER TABLE Production_Product ADD CONSTRAINT FK_Product_ProductSubcategory 
    FOREIGN KEY (ProductSubcategoryID) REFERENCES Production_ProductSubcategory(ProductSubcategoryID);

ALTER TABLE Production_Product ADD CONSTRAINT FK_Product_UnitMeasure_SizeUnitMeasureCode 
    FOREIGN KEY (SizeUnitMeasureCode) REFERENCES Production_UnitMeasure(UnitMeasureCode);

ALTER TABLE Production_Product ADD CONSTRAINT FK_Product_UnitMeasure_WeightUnitMeasureCode 
    FOREIGN KEY (WeightUnitMeasureCode) REFERENCES Production_UnitMeasure(UnitMeasureCode);

ALTER TABLE Production_ProductCostHistory ADD CONSTRAINT FK_ProductCostHistory_Product 
    FOREIGN KEY (ProductID) REFERENCES Production_Product(ProductID);

ALTER TABLE Production_ProductDocument ADD CONSTRAINT FK_ProductDocument_Product 
    FOREIGN KEY (ProductID) REFERENCES Production_Product(ProductID);

ALTER TABLE Production_ProductDocument ADD CONSTRAINT FK_ProductDocument_Document 
    FOREIGN KEY (DocumentNode) REFERENCES Production_Document(DocumentNode);

ALTER TABLE Production_ProductInventory ADD CONSTRAINT FK_ProductInventory_Product 
    FOREIGN KEY (ProductID) REFERENCES Production_Product(ProductID);

ALTER TABLE Production_ProductInventory ADD CONSTRAINT FK_ProductInventory_Location 
    FOREIGN KEY (LocationID) REFERENCES Production_Location(LocationID);

ALTER TABLE Production_ProductListPriceHistory ADD CONSTRAINT FK_ProductListPriceHistory_Product 
    FOREIGN KEY (ProductID) REFERENCES Production_Product(ProductID);

ALTER TABLE Production_ProductModelIllustration ADD CONSTRAINT FK_ProductModelIllustration_ProductModel 
    FOREIGN KEY (ProductModelID) REFERENCES Production_ProductModel(ProductModelID);

ALTER TABLE Production_ProductModelIllustration ADD CONSTRAINT FK_ProductModelIllustration_Illustration 
    FOREIGN KEY (IllustrationID) REFERENCES Production_Illustration(IllustrationID);

ALTER TABLE Production_ProductModelProductDescriptionCulture ADD CONSTRAINT FK_ProductModelProductDescriptionCulture_ProductModel 
    FOREIGN KEY (ProductModelID) REFERENCES Production_ProductModel(ProductModelID);

ALTER TABLE Production_ProductModelProductDescriptionCulture ADD CONSTRAINT FK_ProductModelProductDescriptionCulture_ProductDescription 
    FOREIGN KEY (ProductDescriptionID) REFERENCES Production_ProductDescription(ProductDescriptionID);

ALTER TABLE Production_ProductModelProductDescriptionCulture ADD CONSTRAINT FK_ProductModelProductDescriptionCulture_Culture 
    FOREIGN KEY (CultureID) REFERENCES Production_Culture(CultureID);

ALTER TABLE Production_ProductProductPhoto ADD CONSTRAINT FK_ProductProductPhoto_Product 
    FOREIGN KEY (ProductID) REFERENCES Production_Product(ProductID);

ALTER TABLE Production_ProductProductPhoto ADD CONSTRAINT FK_ProductProductPhoto_ProductPhoto 
    FOREIGN KEY (ProductPhotoID) REFERENCES Production_ProductPhoto(ProductPhotoID);

ALTER TABLE Production_ProductReview ADD CONSTRAINT FK_ProductReview_Product 
    FOREIGN KEY (ProductID) REFERENCES Production_Product(ProductID);

ALTER TABLE Production_ProductSubcategory ADD CONSTRAINT FK_ProductSubcategory_ProductCategory 
    FOREIGN KEY (ProductCategoryID) REFERENCES Production_ProductCategory(ProductCategoryID);

ALTER TABLE Production_TransactionHistory ADD CONSTRAINT FK_TransactionHistory_Product 
    FOREIGN KEY (ProductID) REFERENCES Production_Product(ProductID);

ALTER TABLE Production_WorkOrder ADD CONSTRAINT FK_WorkOrder_Product 
    FOREIGN KEY (ProductID) REFERENCES Production_Product(ProductID);

ALTER TABLE Production_WorkOrder ADD CONSTRAINT FK_WorkOrder_ScrapReason 
    FOREIGN KEY (ScrapReasonID) REFERENCES Production_ScrapReason(ScrapReasonID);

ALTER TABLE Production_WorkOrderRouting ADD CONSTRAINT FK_WorkOrderRouting_WorkOrder 
    FOREIGN KEY (WorkOrderID) REFERENCES Production_WorkOrder(WorkOrderID);

ALTER TABLE Production_WorkOrderRouting ADD CONSTRAINT FK_WorkOrderRouting_Location 
    FOREIGN KEY (LocationID) REFERENCES Production_Location(LocationID);

ALTER TABLE Purchasing_ProductVendor ADD CONSTRAINT FK_ProductVendor_Product 
    FOREIGN KEY (ProductID) REFERENCES Production_Product(ProductID);

ALTER TABLE Purchasing_ProductVendor ADD CONSTRAINT FK_ProductVendor_Vendor 
    FOREIGN KEY (BusinessEntityID) REFERENCES Purchasing_Vendor(BusinessEntityID);

ALTER TABLE Purchasing_ProductVendor ADD CONSTRAINT FK_ProductVendor_UnitMeasure 
    FOREIGN KEY (UnitMeasureCode) REFERENCES Production_UnitMeasure(UnitMeasureCode);

ALTER TABLE Purchasing_PurchaseOrderDetail ADD CONSTRAINT FK_PurchaseOrderDetail_PurchaseOrderHeader 
    FOREIGN KEY (PurchaseOrderID) REFERENCES Purchasing_PurchaseOrderHeader(PurchaseOrderID);

ALTER TABLE Purchasing_PurchaseOrderDetail ADD CONSTRAINT FK_PurchaseOrderDetail_Product 
    FOREIGN KEY (ProductID) REFERENCES Production_Product(ProductID);

ALTER TABLE Purchasing_PurchaseOrderHeader ADD CONSTRAINT FK_PurchaseOrderHeader_Employee 
    FOREIGN KEY (EmployeeID) REFERENCES HumanResources_Employee(BusinessEntityID);

ALTER TABLE Purchasing_PurchaseOrderHeader ADD CONSTRAINT FK_PurchaseOrderHeader_Vendor 
    FOREIGN KEY (VendorID) REFERENCES Purchasing_Vendor(BusinessEntityID);

ALTER TABLE Purchasing_PurchaseOrderHeader ADD CONSTRAINT FK_PurchaseOrderHeader_ShipMethod 
    FOREIGN KEY (ShipMethodID) REFERENCES Purchasing_ShipMethod(ShipMethodID);

ALTER TABLE Purchasing_Vendor ADD CONSTRAINT FK_Vendor_BusinessEntity 
    FOREIGN KEY (BusinessEntityID) REFERENCES Person_BusinessEntity(BusinessEntityID);

ALTER TABLE HumanResources_Employee ADD CONSTRAINT FK_Employee_Person 
    FOREIGN KEY (BusinessEntityID) REFERENCES Person_Person(BusinessEntityID);

ALTER TABLE HumanResources_EmployeeDepartmentHistory ADD CONSTRAINT FK_EmployeeDepartmentHistory_Employee 
    FOREIGN KEY (BusinessEntityID) REFERENCES HumanResources_Employee(BusinessEntityID);

ALTER TABLE HumanResources_EmployeeDepartmentHistory ADD CONSTRAINT FK_EmployeeDepartmentHistory_Department 
    FOREIGN KEY (DepartmentID) REFERENCES HumanResources_Department(DepartmentID);

ALTER TABLE HumanResources_EmployeeDepartmentHistory ADD CONSTRAINT FK_EmployeeDepartmentHistory_Shift 
    FOREIGN KEY (ShiftID) REFERENCES HumanResources_Shift(ShiftID);

ALTER TABLE HumanResources_EmployeePayHistory ADD CONSTRAINT FK_EmployeePayHistory_Employee 
    FOREIGN KEY (BusinessEntityID) REFERENCES HumanResources_Employee(BusinessEntityID);

ALTER TABLE HumanResources_JobCandidate ADD CONSTRAINT FK_JobCandidate_Employee 
    FOREIGN KEY (BusinessEntityID) REFERENCES HumanResources_Employee(BusinessEntityID);