# -*- coding: utf-8 -*-
"""
Autor: Aron Ifanger Maciel
Contato: aronifanger@gmail.com
Data: 19/01/2020
"""
import pandas as pd

INPUT_MOV_TABLE_NAME = "MovtoITEM.xlsx"
INPUT_BALANCE_TABLE_NAME = "SaldoITEM.xlsx"
OUTPUT_BALANCE_TABLE_NAME = "BalncITEM.xlsx"

def main():
    # reading databases
    mvto = pd.read_excel(INPUT_MOV_TABLE_NAME)
    sldo = pd.read_excel(INPUT_BALANCE_TABLE_NAME)

    # getting reference dates
    start_date, end_date = sldo["data_inicio"].min(), sldo["data_final"].min()
    # stock reference date is one day before first movement
    stock_ref_date = start_date - pd.DateOffset(days=1)

    # filtering period
    mvto = mvto[(mvto["data_lancamento"]>=start_date)&(mvto["data_lancamento"]<=end_date)].reset_index(drop=True)

    if sldo.shape[0] != len(sldo["item"].unique()):
        print("There are duplicated items in balance table.")
    if sldo.shape[0] != len(mvto["item"].unique()):
        print("The items from balance table are different from movement table items.")

    # generating final grid
    all_items = mvto.item.unique()
    all_days = [stock_ref_date + pd.DateOffset(days=i) for i in range((end_date - stock_ref_date).days + 1)]
    all_rows = [(item, date) for date in all_days for item in all_items]
    stock_evolution = pd.DataFrame(all_rows, columns=["item", "data_lancamento"])
    stock_evolution.sample(5)

    # merging balance table with movement table
    first_mvto = sldo[["item","data_inicio","qtd_inicio","valor_inicio"]].copy()
    first_mvto = first_mvto.rename({"data_inicio":"data_lancamento", 
                                    "qtd_inicio":"quantidade", 
                                    "valor_inicio":"valor" }, axis="columns")
    first_mvto["tipo_movimento"] = "Ent"
    first_mvto = first_mvto[["item","tipo_movimento","data_lancamento","quantidade","valor"]]
    first_mvto["data_lancamento"] = stock_ref_date
    mvto_updated = pd.concat([first_mvto, mvto]).reset_index(drop=True)

    # grouping movements by (item, date, movement_type) touple
    grouped_data = mvto_updated.groupby(["item", "data_lancamento", "tipo_movimento"], as_index=False)\
                    .agg({"quantidade":sum, "valor":sum})
    # putting movement type in different columns
    agg_data = pd.pivot_table(grouped_data, values=["quantidade", "valor"], 
                              index=["item", "data_lancamento"], 
                              columns=["tipo_movimento"], 
                              aggfunc=sum).reset_index()
    agg_data.columns = [c for c, _ in agg_data.columns[:2]] + ["{}_{}".format(c,d) for c, d in agg_data.columns[2:]]
    agg_data = agg_data.rename({"quantidade_Ent":"quantidade_entrada",
                                "quantidade_Sai":"quantidade_saida",
                                "valor_Ent":"valor_entrada",
                                "valor_Sai":"valor_saida"}, axis="columns").fillna(0)
    # evaluating daily results
    agg_data["resultado_quantidade"] = agg_data["quantidade_entrada"] - agg_data["quantidade_saida"]
    agg_data["resultado_valor"] = agg_data["valor_entrada"] - agg_data["valor_saida"]

    # merging with stock evolution grid to prevent missing dates
    agg_data = stock_evolution.merge(agg_data, on=["item", "data_lancamento"]).fillna(0).reset_index(drop=True)

    # evaluating cummulative balance
    cum_data = agg_data.groupby(by=['item','data_lancamento'])\
                       .agg({"resultado_quantidade":"sum", "resultado_valor":"sum"})\
                       .groupby(level=[0])\
                       .cumsum()\
                       .reset_index()\
                       .rename({"resultado_quantidade":"saldo_final_quantidade", 
                                "resultado_valor":"saldo_final_valor"}, axis="columns")

    # merging cummulative in main table
    final_data = agg_data.merge(cum_data, on=['item','data_lancamento']).sort_values(['item','data_lancamento'])  
    final_data["saldo_inicial_quantidade"] = 0
    final_data["saldo_inicial_valor"] = 0
    final_data["saldo_inicial_quantidade"] = [None] + final_data["saldo_final_quantidade"].iloc[:-1].tolist()
    final_data["saldo_inicial_valor"] = [None] + final_data["saldo_final_valor"].iloc[:-1].tolist()
    final_data = final_data[final_data["data_lancamento"] != stock_ref_date].reset_index(drop=True)
    final_data = final_data[["item","data_lancamento",
                             "quantidade_entrada","valor_entrada",
                             "quantidade_saida","valor_saida",
                             "saldo_inicial_quantidade","saldo_inicial_valor",
                             "saldo_final_quantidade","saldo_final_valor"]]\
                            .sort_values(['data_lancamento','item'])\
                            .reset_index(drop=True)

    # getting final balance
    last_mvto = sldo[["item","data_final","qtd_final","valor_final"]]\
                           .rename({"data_final":"data_lancamento"}, axis="columns").copy()
    # filter final balance from movement table
    ft = final_data["data_lancamento"] == end_date
    check_result = last_mvto.merge(final_data[ft], on=["item","data_lancamento"])\
                    [["item","qtd_final","valor_final","saldo_final_quantidade","saldo_final_valor"]]
    # check if values and quantities are equals
    check_result["check_qtdd"] = abs(check_result["qtd_final"] - check_result["saldo_final_quantidade"]) < 1e-3
    check_result["check_value"] = abs(check_result["valor_final"] - check_result["saldo_final_valor"]) < 1e-3
    # count wrong results
    wrong_qtt = check_result["check_qtdd"].sum() - len(check_result["check_qtdd"])
    wrong_vl = check_result["check_value"].sum() - len(check_result["check_value"])
    # print summary
    print("There are {} wrong quantities in final date balance.".format(wrong_qtt))
    print("There are {} wrong values in final date balance.".format(wrong_vl))

    # save final table
    final_data.to_excel(OUTPUT_BALANCE_TABLE_NAME)
    
if __name__ == '__main__':
    main()