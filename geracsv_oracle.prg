%##################      HOJE - 25.01.2013      ####################
%##################      HOJE - 25.01.2013      ####################
%##################      HOJE - 25.01.2013      ####################
%# 
%#   Gera docs CSV do Contrato e Contrato_Item para exportar 
%#   no Oracle
%#
%#   * O Doc deve ser formatado(LI ALL FORMAT) na mesma ordem que
%#     estao as colunas na tabela no lado do ORACLE. 
%#   * Para listar a ordem no Oracle, execute:
%#     SELECT COLUMN_NAME || ',\' FROM USER_TAB_COLUMNS  
%#            WHERE TABLE_NAME='CONTRATO_ITEM' ORDER BY COLUMN_ID;  
%# 
%$   By Ivan  - JAN/2013 
%###################################################################
localprocedure gerasets(phost, pdir_dest, pdt_venda)

 find contrato_item wh dt_venda = pdt_venda -> sconi 
 find sconi con_item contrato keep contrato -> scon 
 if $Setcount = 0  
    out 'Dados nao encontrados...'
    return
 endif 

 set out docspool
 LI ALL scon FORMAT \
     NR_CONTRATO,\                    
     SG_LOJA,\                        
     TP_FIN,\                         
     DT_VENDA,\                       
     CD_CLIENTE,\                     
     CD_REPRESENTANTE,\               
     TP_CONTRATO,\                    
     CD_AVALISTA,\                    
     NR_MINUTA,\                      
     NR_QUITACAO,\                    
     NR_RECIBO,\                      
     NR_ANOREF,\                      
     NR_MESREF,\                      
     VL_VENDA,\                       
     VL_CONTRATO,\                    
     TP_MOEDA,\                       
     VL_MOEDA,\                       
     VL_DESP_FIN,\                    
     VL_SALDO,\                       
     VL_SALDO_MOEDA,\                 
     QT_PRESTACAO,\                   
     QT_PREST_ATRASO,\                
     QT_PREST_PAGAS,\                 
     NR_DIAS_MATRASO,\                
     TT_DIAS_ATRASO,\                 
     TP_PROMOCAO,\                    
     TT_AZUL,\                        
     TT_VERDE,\                       
     TT_VERMELHO,\                    
     FL_SITUACAO,\                    
     VL_TOTAL_DESC,\                  
     VL_PERC_DESC,\                   
     CD_REGIAO,\                      
     CD_AREA_COBRANCA,\               
     TP_DOLAR,\                       
     VL_DOLAR,\                       
     FL_CARTAO_CARENCIA,\             
     FL_CARTAO_PE,\                   
     TP_SERIE,\                       
     NR_NFISCAL,\                     
     NR_COMP_DEBREC,\                 
     DT_CANCEL,\                      
     TP_CANCEL,\                      
     CD_NAT_OPE,\                     
     VL_ENTRADA,\                     
     FL_CONCEITO_FINAL,\              
     TP_FINANC,\                      
     CD_AREA_ENTREGA,\                
     FL_SEST,\                        
     NR_NOTA_SRE,\                    
     VL_ARREDONDA,\                   
     CD_VENDEDOR,\                    
     SQ_PRO_VENDA,\                   
     CD_DIVISAO,\                     
     SQ_DEPENDENTE,\                  
     TP_SERIE_A,\                     
     NR_NOTA_SERIE_A,\                
     VL_SERIE_A,\                     
     VL_PERC_DESC_ICMS,\              
     TP_MINUTA,\                      
     FL_ALTERADO,\                    
     CD_PLANO,\                       
     VL_RESTITUIDO,\                  
     NR_MES_EXCLUI,\                  
     NR_ANO_EXCLUI,\                  
     NR_SECAO,\                       
     VL_MOEDA_VENDA,\                 
     DT_EXCLUI,\                      
     NR_ANOREF_CAN,\                  
     NR_MESREF_CAN,\                  
     NR_NOTA_ORIGEM,\                 
     TP_SERIE_ORIGEM,\                
     VL_DESCONTO_MOEDA,\              
     VL_DESP_FIN_MOEDA,\              
     FL_COBRANCA,\                    
     FL_COBROU,\                      
     TT_DIAS_UATRASO,\                
     TT_DIAS_ANTEC,\                  
     TP_REC_MAN_AUT,\                 
     FL_RETORNO,\                     
     DT_QUITACAO,\                    
     DT_DIGITACAO,\                   
     CD_MATRICULA,\                   
     DT_CAIXA,\                       
     SG_LOJA_CHEQUE,\                 
     FL_CONJUGADO,\                   
     FL_MODOPAGTO,\                   
     QT_CHEQUES,\                     
     VL_1PARCELA,\                    
     SG_PREMINUTA,\                   
     NR_PREMINUTA,\                   
     CF_NUMERO,\                      
     CF_SERIE,\                       
     CF_ECF,\                         
     CF_IMP,\                         
     CD_TERMCF,\                      
     NR_ADIANT,\                      
     VL_GARANTIA,\                    
     VL_GARANTIADF,\                  
     NR_CONTRATOR,\                   
     VL_CFCONTRATO,\                  
     VL_CFDESPFIN,\                   
     CF_SERMFD,\                      
     CD_CHAVE,\                       
     CD_CHAVE_CON,\                   
     CD_CHAVE_CLI,\                   
     CD_CHAVE_NT,\                    
     CD_CHAVE_VEN,\                   
     CD_CHAVE_CAN,\                   
     CD_CHAVE_EXC,\                   
     CD_CHAVE_AVAL,\                  
     CD_CHAVE_PAG,\                   
     CD_CHAVE_LDTV,\                  
     CD_CHAVE_PREMIN,\                
     CD_CHAVE_CONR
 Set out terminal
 transfrcp('CONTRATO', phost,pdir_dest) 

 Set out Docspool
 LI ALL sconi FORMAT \
      NR_CONTRATO,\                    
      SG_LOJA,\                        
      TP_FIN,\                         
      CD_ALMOXARIFADO,\                
      CD_GRUPO,\                       
      CD_PRODUTO,\                     
      NR_NOTA,\                        
      TP_SERIE,\                       
      VL_PRECO_UNI,\                   
      QT_PRODUTO,\                     
      CD_VENDEDOR,\                    
      SG_SAI_DE,\                      
      VL_PRECO_TOTAL,\                 
      CD_PRECO,\                       
      NR_TABELA,\                      
      NR_TAB_PRECO,\                   
      TP_CONTRATO,\                    
      DT_ENTREGA,\                     
      NR_MERC,\                        
      VL_PRECO_REAL,\                  
      VL_TOTAL_SOL,\                   
      NR_MATRICULA,\                   
      VL_ALIQ_ICMS,\                   
      TP_FINANC,\                      
      NR_MINUTA,\                      
      NR_SEQUENCIA,\                   
      TP_MINUTA,\                      
      CD_SECAO,\                       
      VL_PRE_UNI_URV,\                 
      CD_REPRESENTANTE,\               
      CD_COR,\                         
      DT_VENDA,\                       
      NR_ITEM,\                        
      FL_PROMOCAO,\                    
      VL_DESCONTO,\                    
      FL_EMBALADA,\                    
      NR_SERIALNUMBER,\                
      VL_BASE_ICMS,\                   
      VL_ICMS,\                        
      NR_CRM,\                         
      NR_ANO,\                         
      CD_GARANTIA,\                    
      VL_CUSTO_AON,\                   
      VL_PRECO_VENDA,\                 
      VL_GARANTIA,\                    
      CD_CFOP,\                        
      VL_DESC_RATEIO,\                 
      VL_CONTABIL,\                    
      CD_TAMANHO,\                     
      CD_CHAVE,\                       
      CD_CHAVE_CT,\                    
      CD_CHAVE_CON,\                   
      CD_CHAVE_MIN
  Set out terminal
  transfrcp('CONTRATO_ITEM', phost,pdir_dest) 
endprocedure

localprocedure transfrcp(filename, host, dir_dest)
System $Concat('su imprime -c "rcp ',$trim($workpath),'/docspool '\
                    ,$Trim(host),':',$trim(dir_dest),'/',          \
                     $Trim($toupper(filename)),'.csv',' "')
out $concat("ENVIADO PARA: ",\
    $trim(host),":",$trim(dir_dest),"/",$trim($toupper(filename)),'.csv')
endprocedure

SET save
SET PAUSE OFF 
SET heading off
%SET nullvalue "null"
SET output fieldlistwidth off
SET output maskfield off
SET output virtualfield on
SET output format commadelimited
SET delimiter ";"
SET textdelimiter "|"

gerasets('scoprog', '/u3/ivan', ($Date-1))

SET restore
Set OutPut Format NORMAL
