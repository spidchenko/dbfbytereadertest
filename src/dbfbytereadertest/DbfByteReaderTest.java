/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbfbytereadertest;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author spidchenko.d
 */

class Field{
    private String name;    //Название столбца
    private int size;       //Ширина столбца в байтах
    private int offset;     //Смещение от начала записи в байтах
        
    public Field(String newName, int newSize, int newOffset){
        name = newName;
        size = newSize;
        offset = newOffset;
    }
        
    static Field[] initializeFieldsArray(int fieldsCount){
        Field [] fieldArrayReturn = new Field [fieldsCount];  //Массив столбцов
        
        for (int i = 0; i< fieldArrayReturn.length; i++){
                fieldArrayReturn[i] = new Field("", 0, 0);
            }
        return fieldArrayReturn;
    }
//<editor-fold defaultstate="collapsed" desc="SETERS-GETERS">
    void setName(String newName){
        name = newName;
    }
    
    void setSize(int newSize){
        size = newSize;
    }
    
    void setOffset(int newOffset){
        offset = newOffset;
    }
    
    String getName(){
        return name;
    }
    
    int getSize(){
        return size;
    }
    int getOffset(){
        return offset;
    }
//</editor-fold>
}

class DbfFile {
    static final int HEADER_FIELD_LENGTH = 32;  //Длина описания столбца - 32 байта
    static final int CURRENT_FIELD_NAME = 9;    //0-9 байты
    static final int CURRENT_FIELD_LENGTH = 16; //16й байт с длиной текущего столбца
    static final Charset FILE_CHARSET = Charset.forName("cp866");//Кодировка
    private int endOfHeaderOffset = 0;           //Смещение в байтах конца заголовка
    private int numOfFields = 0;                 //Количество столбцов таблицы
    private int numOfRecords = 0;                //Количество записей в таблице
    private int oneRecordLength = 0;             //Длина одной записи в байтах
    private Field [] fieldArray;
        //------
    private String filePath = "";//E:\\0302.dbf"; 
    private Object[][] tableData;               //Данные для отображения в jTable
        
    public DbfFile(String filePathToOpen){
        filePath = filePathToOpen;
        //--
        FileInputStream inputStream = null;
        byte[] byteBufferArray = new byte[1024];   //Байтовый буфер для чтения. Самая длинная запись которую я видел - 505 байт, пусть будет в 2 раза больше
                
        try{
            inputStream = new FileInputStream(filePath);
            inputStream.read(byteBufferArray, 0, 32);
            //Делаем unsigned byte массив [4-7] байтов (количество записей, старший байт справа)
            int [] byteArray = new int[4];
            for(int i = 0; i<4; i++){
                byteArray[i] = (byteBufferArray[i+4]>0)?byteBufferArray[i+4]:(byteBufferArray[i+4] & 0xFF);
            }
            //сдвигаем байты (старший слева) и получаем количество записей (32бит число)
            numOfRecords = byteArray[0]|(byteArray[1]<<8)|(byteArray[2]<<16)|(byteArray[3]<<24);
            //Делаем unsigned byte массив [10-11] байтов (длина одной записи, старший байт справа)
            for(int i = 0; i<2; i++){
                byteArray[i] = (byteBufferArray[i+10]>0)?byteBufferArray[i+10]:(byteBufferArray[i+10] & 0xFF);
            }           
            //сдвигаем байты (старший слева) и получаем длину одной записи (16бит число)
            oneRecordLength = byteArray[0]|(byteArray[1]<<8);
                                
            while(inputStream.read()!=0xD){     //Поиск конца заголовка: байт 0xD
                endOfHeaderOffset++;
            }
        
            //Считаем количество столбцов в таблице
            numOfFields = endOfHeaderOffset/HEADER_FIELD_LENGTH; 
        
            inputStream = new FileInputStream(filePath);  //Откроем еше раз, чтобы вернуться в начало файла, как иначе хз
            inputStream.skip(32);       //Пропустили заголовок

        //Парсим описания столбцов таблицы (fieldArray):
            fieldArray = Field.initializeFieldsArray(numOfFields);  //Инициализируем массив столбцов
            for (int i = 0; i < fieldArray.length; i++){
                inputStream.read(byteBufferArray, 0, HEADER_FIELD_LENGTH);    //32 байта 
                //Название столбца (вытащили из байтового массива и убрали пробелы с конца одной коммандой! >:3 )
                //new String корректно отработает с default charset ASCII, на линуксе или в Японии с UTF Default будут проблемы 
                fieldArray[i].setName(new String(Arrays.copyOfRange(byteBufferArray, 0, CURRENT_FIELD_NAME)).trim());  //9 байт
                //Размер столбца
                if (byteBufferArray[CURRENT_FIELD_LENGTH]>0){
                    fieldArray[i].setSize(byteBufferArray[CURRENT_FIELD_LENGTH]);
                } else{
                    fieldArray[i].setSize(byteBufferArray[CURRENT_FIELD_LENGTH] & 0xFF);
                }
                //Сдвиг от начала записи в байтах
                if (i != 0){
                    fieldArray[i].setOffset(fieldArray[i-1].getOffset()+fieldArray[i-1].getSize());
                } else{
                    fieldArray[i].setOffset(0);
                }
            }
        //---    

        //Парсим строки таблицы (tableData):
            String currentLine = "";
            //Файловый курсор сейчас перед 0xD, пропустим [0xD, 0x0]
            inputStream.skip(2);
            
            tableData = new String [numOfRecords][numOfFields];
            for (int i = 0; i < numOfRecords; i++){
                //Считали одну запись в byteBufferArray
                inputStream.read(byteBufferArray, 0, oneRecordLength);
                //Декодировали массив byteBufferArray, обернутый в байтбуффер в UTF-16 
                //Имеем на выходе строку UTF-16 с полями из DBF файла
                currentLine = FILE_CHARSET.decode(ByteBuffer.wrap(byteBufferArray,0, oneRecordLength)).toString();
                for(int j =0; j < numOfFields; j++){
                    tableData[i][j] = currentLine.substring(fieldArray[j].getOffset()+1, //Почему смещение на 1 байт вправо?
                                                                    fieldArray[j].getOffset() + fieldArray[j].getSize()+1).trim();
                }
            }
        //---    
            
            /*for (int i = 0; i < numOfRecords; i++){
            //Считали одну запись в byteBufferArray
            inputStream.read(byteBufferArray, 0, oneRecordLength);
            //Декодировали массив byteBufferArray, обернутый в байтбуффер в UTF-16
            //Имеем на выходе строку UTF-16 с полями из DBF файла
            System.out.println(FILE_CHARSET.decode(ByteBuffer.wrap(byteBufferArray,0, oneRecordLength)));
            }*/
            
        } catch (IOException ex) {
            Logger.getLogger(DbfFile.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (inputStream != null) {try {
                inputStream.close();
                } catch (IOException ex) {
                    Logger.getLogger(DbfFile.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
            //--
    }//End of DbfFile Constructor method
    
    int getNumOfRecords(){
        return numOfRecords;
    }
    int getNumOfFields(){
        return numOfFields;
    }
    
    Field[] getFieldArray(){
        return fieldArray;
    }
    
    Object[][] getTableDataToShow(){
        return tableData;  
    }   
    
    int getOneRecordLength(){
        return oneRecordLength;
    }
    
    void printFileInfo(){
        System.out.print("\n");
        System.out.format("endOfHeaderOffset:%4d \nnumOfFields:%3d \nnumOfRecords:%4d \noneRecordLength:%4d\n\n", endOfHeaderOffset, numOfFields, numOfRecords, oneRecordLength);
            
        for (Field fieldArray1 : fieldArray) {
            System.out.format("%10s | %5d | %5d \n", fieldArray1.getName(), fieldArray1.getSize(), fieldArray1.getOffset());
        }
        System.out.print("\n");        
    }
}

public class DbfByteReaderTest {

    /**
     * @param args the command line arguments
     */
    


    public static void main(String[] args) {
        DbfFile currentDbf = new DbfFile("E:\\0302.dbf");
        currentDbf.printFileInfo();
                
        for(int i = 0; i < currentDbf.getNumOfRecords(); i++){
            for(int j = 0; j < currentDbf.getNumOfFields(); j++){
                System.out.printf("%"+currentDbf.getFieldArray()[j].getSize()+"s",currentDbf.getTableDataToShow()[i][j]);
            }            
            System.out.print("\n");
        }

                
    }
}
