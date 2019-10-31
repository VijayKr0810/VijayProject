import pandas as pd
from xlrd import open_workbook


CONFIG_FILE="config.cfg"


file_ptr=open(CONFIG_FILE,'r')
#data = file_ptr.read()
#print data

for line in file_ptr:
    print line
    
print file_ptr.closed

file_ptr.close()



#df = pd.read_csv (r'Path where the CSV file is stored\File name.csv')

#df = pd.read_excel ('io_card_det.xlsx') #for an earlier version of Excel, you may need to use the file extension of 'xls'
#print (df)

"""
card_det_dict={"card_id":"",
               "card_type":""
    }

wb = open_workbook('io_card_det.xlsx')

for sheet in wb.sheets():
    num_rows=sheet.nrows
    num_col=sheet.ncols
    print num_rows,num_col
    
    row_list=[]
    for row_idx in range (1,num_rows):
        #col_list={}
        #for col_idx in range (0,num_col):
        card_det_dict.clear()
        card_det_dict["card_id"]=(sheet.cell(row_idx,0).value)
        card_det_dict["card_type"]=(sheet.cell(row_idx,1).value)
        row_list.append(card_det_dict)
            
            #data =  "row_idx : %d ,col_idx :%d ,value %s"%(row_idx,col_idx,value)
            #print data
    

    print row_list


row_data=pd.DataFrame(df, columns= ['card_id'])

total_row=len(row_data)

for row_idx in range (1,2):
    value  = (s.cell(row,col).value)
    
    row_data=pd.DataFrame(df,columns= ['card_id'])#,row=row_idx
    print len(row_data)

"""
