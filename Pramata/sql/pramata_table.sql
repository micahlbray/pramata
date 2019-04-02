drop table MIP2.PRAMATA_DETAIL
create table MIP2.PRAMATA_DETAIL (
		pramata_number int
		,parent_pramata_number int
		,contract_type nvarchar(255)
		,pdf_url nvarchar(500)
		,Address_Of_Leased_Space_Address_1 nvarchar(500)
		,Address_Of_Leased_Space_City nvarchar(255)
		,Address_Of_Leased_Space_State_Province nvarchar(255)
		,Address_Of_Leased_Space_Zip_Postal_Code nvarchar(500)
		,Company_Group nvarchar(255)
		,Contract_Model nvarchar(255)
		,Document_Title nvarchar(255)
		,Document_Type nvarchar(500)
		,Effective_Date datetime
		,Amendments_Amended_Term nvarchar(255)
		,Amendments_Original_Term nvarchar(255)
		,Area nvarchar(max)
		,Building_Id nvarchar(500)
		,Contract_Status_Additional_Information nvarchar(max)
		,Contract_Status_Referenced_Document nvarchar(500)
		,Contract_Status_Status nvarchar(500)
		,Courtesy_Services nvarchar(500)
		,Customer_Signatory_Name nvarchar(255)
		,Customer_Signatory_Title nvarchar(255)
		,Fee nvarchar(500)
		,Initial_Validation nvarchar(max)
		,Mdu nvarchar(500)
		,National_Account nvarchar(500)
		,Notes nvarchar(max)
		,Owner_Occupied nvarchar(500)
		,Parcel_Number int
		,Qa_New_Term_All_Boolean int
		,Qa_New_Term_All_Document_Type nvarchar(500)
		,Qa_New_Term_All_Link_Term nvarchar(500)
		,Qa_New_Term_All_Select_Box nvarchar(255)
		,Qa_New_Term_Document_Term nvarchar(500)
		,Qa_New_Term_Link_Term nvarchar(500)
		,Referral_Partner nvarchar(500)
		,Region_Imported nvarchar(500)
		,Signature_Status_Status nvarchar(500)
		,Signature_Status_Which_Party nvarchar(500)
		,Site_Contact_Contact_Email nvarchar(500)
		,Site_Contact_Contact_Number nvarchar(255)
		,Site_Contact_Name nvarchar(255)
		,Term_Renewal_And_Expiration_Dates_Date_Type nvarchar(500)
		,Term_Renewal_And_Expiration_Dates_Key_Dates datetime
		,Term_Renewal_And_Expiration_Dates_Note nvarchar(max)
		,Term_Renewal_And_Expiration_Dates_Notice_Period nvarchar(255)
		,Term_Renewal_And_Expiration_Dates_Term_Details_And_Renewals nvarchar(500)
		,Term_Renewal_And_Expiration_Dates_Term_Months int
		,Validation_Resolution nvarchar(max)
		,Validator nvarchar(500)
		,LoadedOn datetime
		,LoadedBy nvarchar(255)
		)

create table MIP2.PRAMATA_NUMBERS (
		pramata_number int
		,is_deleted nvarchar(255)
		,start_date_timestamp datetime
		,end_date_timestamp datetime
		,LoadedOn datetime
		,LoadedBy nvarchar(255)
		)


create table MIP2.PRAMATA_NUMBER_ADDRESS(
		pramata_number int
		,Address_Number int
		,Address_Of_Leased_Space_Address_1 nvarchar(500)
		,Address_Of_Leased_Space_City nvarchar(255)
		,Address_Of_Leased_Space_State_Province nvarchar(255)
		,Address_Of_Leased_Space_Zip_Postal_Code nvarchar(500)
		,LoadedOn datetime
		,LoadedBy nvarchar(255)
		)
drop table MIP2.PRAMATA_NUMBER_TERM
create table MIP2.PRAMATA_NUMBER_TERM(
		pramata_number int
		,Term_Number int
		,Term_Renewal_And_Expiration_Dates_Date_Type nvarchar(500)
		,Term_Renewal_And_Expiration_Dates_Key_Dates datetime
		,Term_Renewal_And_Expiration_Dates_Note nvarchar(max)
		,Term_Renewal_And_Expiration_Dates_Notice_Period nvarchar(255)
		,Term_Renewal_And_Expiration_Dates_Term_Details_And_Renewals nvarchar(500)
		,Term_Renewal_And_Expiration_Dates_Term_Months int
		,LoadedOn datetime
		,LoadedBy nvarchar(255)
		)

create table MIP2.PRAMATA_NUMBER_SIGNATORY(
		pramata_number int
		,Signatory_Number int
		,Customer_Signatory_Name nvarchar(255)
		,Customer_Signatory_Title nvarchar(255)
		,LoadedOn datetime
		,LoadedBy nvarchar(255)
		)

create table MIP2.PRAMATA_NUMBER_CONTACT(
		pramata_number int
		,Contact_Number int
		,Site_Contact_Contact_Email nvarchar(500)
		,Site_Contact_Contact_Number nvarchar(255)
		,Site_Contact_Name nvarchar(255)
		,LoadedOn datetime
		,LoadedBy nvarchar(255)
		)

truncate table bi_mip.mip2.PRAMATA_NUMBER_DETAIL
select * from bi_mip.mip2.PRAMATA_NUMBER_DETAIL
truncate table MIP2.PRAMATA_NUMBER_ADDRESS
select * from MIP2.PRAMATA_NUMBER_ADDRESS
truncate table MIP2.PRAMATA_NUMBER_TERM
select * from MIP2.PRAMATA_NUMBER_TERM
truncate table MIP2.PRAMATA_NUMBER_SIGNATORY


select distinct 
		month_start
		, month_end
from WISDM.Dim.Date
where month_id >= 201701
	and month_id < 201805
order by month_start


