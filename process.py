from queries import *
from datetime import date
import pandas as pd
from sqlalchemy import create_engine


def execute_query(query: str, parameters: dict):
    ip = '10.181.139.41'
    database = 'Master'
    conn_str = f'mssql+pyodbc://{"USR_CMBB7434"}:{"123"}@{ip}/{database}?driver=SQL+Server'
    engine = create_engine(conn_str)
    formatted_query = query.format(**parameters)
    return pd.read_sql(formatted_query, engine)


def normative_matrix(processdate: date):
    process_date = processdate.strftime('%Y%m%d')
    divisas = execute_query(query=CURR_PAIR, parameters={'process_date': process_date})
    factores_normativos = {}
    for i in divisas.index:
        divisa_activa = divisas["moneda_activa"].iat[i]
        divisa_pasiva = divisas["moneda_pasiva"].iat[i]
        if divisa_activa == divisa_pasiva or (divisa_activa == 'CLP' and divisa_pasiva == 'CLF') or \
                (divisa_activa == 'CLF' and divisa_pasiva == 'CLP'):
            factores_normativos[divisa_activa, divisa_pasiva] = {'1Y': 0, '5Y': 0.5, 'INF': 1.5}
        elif divisa_activa not in ('COP', 'PEN', 'MXN', 'BRL') and divisa_pasiva not in ('COP', 'PEN', 'MXN', 'BRL'):
            factores_normativos[divisa_activa, divisa_pasiva] = {'1Y': 1.5, '5Y': 7, 'INF': 13}
        else:
            factores_normativos[divisa_activa, divisa_pasiva] = {'1Y': 4.5, '5Y': 20, 'INF': 30}
    return factores_normativos


def cptys(processdate):
    query = execute_query(query=CPTY, parameters={'process_date': processdate})
    clientes = query.iloc[:, 0].unique()
    relacion = []
    for i in clientes:
        cliente = query.loc[query['NOMBRE'] == i]
        padre = cliente['NOMBRE_PADRE'].iat[0]
        netting = cliente['NETTING'].iat[0]
        garantia = cliente['GARANTIA'].iat[0]
        relacion.append([i, padre, netting, garantia])
    df_relacion = pd.DataFrame(relacion, columns=["CPTY", "CPTY_PADRE", "NETTING", "GARANTIA"])
    return df_relacion


def swap_factors(factores, processdate: date):
    process_date = processdate.strftime('%Y%m%d')
    operaciones = execute_query(query=DETALLE_SWAP, parameters={'process_date': process_date})
    operaciones_unica = operaciones.iloc[:, 0].unique()
    factores_final = []

    counterparties = cptys(processdate)

    for j in operaciones_unica:
        operacion = operaciones.loc[operaciones['numerooperacion'] == j]
        plazos = operacion.iloc[:, 5].unique()
        moneda_activa = operacion["moneda_activa"].iat[0]
        moneda_pasiva = operacion["moneda_pasiva"].iat[0]
        instrumento = operacion["instrumento"].iat[0]
        factores_op = factores[moneda_activa, moneda_pasiva]
        cliente = operacion["nombre_cliente"].iat[0]
        counterparty = counterparties.loc[counterparties["CPTY"] == cliente]
        padre = counterparty["CPTY_PADRE"].iat[0]
        csa = 'No'
        if padre == 'CHICAGO MERCANTILE EXCHANGE - CME' \
                or padre == 'LCHCLEARNET LIMITED' or padre == 'COMDER CONTRAPARTE CENTRAL SA':
            csa = 'Si'
        for k in plazos:
            if k <= 365:
                factor_plazo = factores_op['1Y']
            elif k <= 1825:
                if csa == 'Si' and factores_op['1Y'] != 0:
                    factor_plazo = factores_op['1Y']
                else:
                    factor_plazo = factores_op['5Y']
            else:
                if csa == 'Si':
                    if factores_op['1Y'] != 0:
                        factor_plazo = factores_op['1Y']
                    else:
                        factor_plazo = factores_op['5Y']
                else:
                    factor_plazo = factores_op['INF']
            op_plazo = operacion.loc[operacion['plazo'] == k]
            amortizacion = op_plazo["amortizacion"].iat[0]
            factores_final.append([j, k, padre, instrumento, moneda_activa, moneda_pasiva, amortizacion,
                                   factor_plazo])

    factores_final_formato = pd.DataFrame(factores_final, columns=["GID", "PLAZO", "CPTY_PADRE",
                                                                   "INSTRUMENTO", "MONEDA_ACTIVA", "MONEDA_PASIVA",
                                                                   "AMORTIZACION", "FACTOR_NORMATIVO"])
    return factores_final_formato


def fx_factors(factores, processdate: date):
    process_date = processdate.strftime('%Y%m%d')
    operaciones = execute_query(query=DETALLE_FX, parameters={'process_date': process_date})
    operaciones_unica = operaciones.iloc[:, 0].unique()
    factores_final = []

    counterparties = cptys(processdate)

    for j in operaciones_unica:
        operacion = operaciones.loc[operaciones['numerooperacion'] == j]
        plazos = operacion.iloc[:, 5].unique()
        moneda_activa = operacion["moneda_activa"].iat[0]
        moneda_pasiva = operacion["moneda_pasiva"].iat[0]
        instrumento = operacion["instrumento"].iat[0]
        factores_op = factores[moneda_activa, moneda_pasiva]
        cliente = operacion["nombre_cliente"].iat[0]
        csa = 'No'
        counterparty = counterparties.loc[counterparties["CPTY"] == cliente]
        padre = counterparty["CPTY_PADRE"].iat[0]
        if padre == 'CHICAGO MERCANTILE EXCHANGE - CME' \
                or padre == 'LCHCLEARNET LIMITED' or padre == 'COMDER CONTRAPARTE CENTRAL SA':
            csa = 'Si'
        for k in plazos:
            if k <= 365:
                factor_plazo = factores_op['1Y']
            elif k <= 1825:
                if csa == 'Si' and factores_op['1Y'] != 0:
                    factor_plazo = factores_op['1Y']
                else:
                    factor_plazo = factores_op['5Y']
            else:
                if csa == 'Si':
                    if factores_op['1Y'] != 0:
                        factor_plazo = factores_op['1Y']
                    else:
                        factor_plazo = factores_op['5Y']
                else:
                    factor_plazo = factores_op['INF']
            op_plazo = operacion.loc[operacion['plazo'] == k]
            amortizacion = op_plazo["amortizacion"].iat[0]
            factores_final.append([j, k, padre, instrumento, moneda_activa, moneda_pasiva, amortizacion,
                                   factor_plazo])

    factores_final_formato = pd.DataFrame(factores_final, columns=["GID", "PLAZO", "CPTY_PADRE",
                                                                   "INSTRUMENTO", "MONEDA_ACTIVA", "MONEDA_PASIVA",
                                                                   "AMORTIZACION", "FACTOR_NORMATIVO"])
    return factores_final_formato


def opt_factors(factores, processdate: date):
    process_date = processdate.strftime('%Y%m%d')
    operaciones = execute_query(query=DETALLE_OPT, parameters={'process_date': process_date})
    operaciones_unica = operaciones.iloc[:, 0].unique()
    factores_final = []

    counterparties = cptys(processdate)

    for j in operaciones_unica:
        operacion = operaciones.loc[operaciones['numerooperacion'] == j]
        plazos = operacion.iloc[:, 5].unique()
        moneda_activa = operacion["moneda_activa"].iat[0]
        moneda_pasiva = operacion["moneda_pasiva"].iat[0]
        instrumento = operacion["instrumento"].iat[0]
        factores_op = factores[moneda_activa, moneda_pasiva]
        cliente = operacion["nombre_cliente"].iat[0]
        csa = 'No'
        counterparty = counterparties.loc[counterparties["CPTY"] == cliente]
        padre = counterparty["CPTY_PADRE"].iat[0]
        if padre == 'CHICAGO MERCANTILE EXCHANGE - CME' \
                or padre == 'LCHCLEARNET LIMITED' or padre == 'COMDER CONTRAPARTE CENTRAL SA':
            csa = 'Si'
        for k in plazos:
            if k <= 365:
                factor_plazo = factores_op['1Y']
            elif k <= 1825:
                if csa == 'Si' and factores_op['1Y'] != 0:
                    factor_plazo = factores_op['1Y']
                else:
                    factor_plazo = factores_op['5Y']
            else:
                if csa == 'Si':
                    if factores_op['1Y'] != 0:
                        factor_plazo = factores_op['1Y']
                    else:
                        factor_plazo = factores_op['5Y']
                else:
                    factor_plazo = factores_op['INF']
            op_plazo = operacion.loc[operacion['plazo'] == k]
            amortizacion = op_plazo["amortizacion"].iat[0]
            factores_final.append([j, k, padre, instrumento, moneda_activa, moneda_pasiva, amortizacion,
                                   factor_plazo])

    factores_final_formato = pd.DataFrame(factores_final, columns=["GID", "PLAZO", "CPTY_PADRE",
                                                                   "INSTRUMENTO", "MONEDA_ACTIVA", "MONEDA_PASIVA",
                                                                   "AMORTIZACION", "FACTOR_NORMATIVO"])
    return factores_final_formato


def cva(processdate: date):
    return execute_query(query=AJUSTE_CVA, parameters={'process_date': processdate})


def swap_r06(processdate: date, factors, aj_cva):
    factores_swap = swap_factors(factors, processdate)
    swap_mtm = execute_query(query=MTM_SWAP, parameters={'process_date': processdate})
    ops = swap_mtm.iloc[:, 0].unique()
    r06 = []
    for i in ops:
        op = factores_swap.loc[factores_swap['GID'] == int(i)]
        cliente = op['CPTY_PADRE'].iat[0]
        mtm_op = swap_mtm.loc[swap_mtm['GID'] == int(i)]
        mtm = mtm_op['MTM'].iat[0]
        cva_op = aj_cva.loc[aj_cva['CONTRATO'] == int(i)]
        try:
            aj = cva_op['Ajuste_CVA'].iat[0]
        except:
            aj = 0
        plazo = op.iloc[:, 1].unique()
        addon = 0
        for j in plazo:
            flujo = op.loc[op['PLAZO'] == j]
            factor = flujo['FACTOR_NORMATIVO'].iat[0] / 100
            amortizacion = flujo['AMORTIZACION'].iat[0]
            addon += factor * amortizacion
        vr = mtm + aj
        vr_plus = max(vr, 0)
        r06_op = vr_plus + addon
        r06.append([int(i), cliente, mtm, aj, vr, vr_plus, addon, r06_op])
    df_r06 = pd.DataFrame(r06, columns=["GID", "CPTY_PADRE", "MTM", "CVA", "VR", "VR+", "ADDON", "R06"])
    file = "outputREC/R06_SWAP_{process_date}.xlsx"
    file = file.format(**{'process_date': processdate})
    excel = pd.ExcelWriter(file)
    df_r06.to_excel(excel, index=False, sheet_name='R06')
    factores_swap.to_excel(excel, index=False, sheet_name='Factors')
    excel.close()
    return df_r06, factores_swap


def fx_r06(processdate: date, factors, aj_cva):
    factores_fx = fx_factors(factors, processdate)
    fx_mtm = execute_query(query=MTM_FX, parameters={'process_date': processdate})
    ops = fx_mtm.iloc[:, 0].unique()
    r06 = []
    for i in ops:
        op = factores_fx.loc[factores_fx['GID'] == int(i)]
        cliente = op['CPTY_PADRE'].iat[0]
        mtm_op = fx_mtm.loc[fx_mtm['GID'] == int(i)]
        mtm = mtm_op['MTM'].iat[0]
        cva_op = aj_cva.loc[aj_cva['CONTRATO'] == int(i)]
        try:
            aj = cva_op['Ajuste_CVA'].iat[0]
        except:
            aj = 0
        plazo = op.iloc[:, 1].unique()
        addon = 0
        for j in plazo:
            flujo = op.loc[op['PLAZO'] == j]
            factor = flujo['FACTOR_NORMATIVO'].iat[0] / 100
            amortizacion = flujo['AMORTIZACION'].iat[0]
            addon += factor * amortizacion
        vr = mtm + aj
        vr_plus = max(vr, 0)
        r06_op = vr_plus + addon
        r06.append([int(i), cliente, mtm, aj, vr, vr_plus, addon, r06_op])
    df_r06 = pd.DataFrame(r06, columns=["GID", "CPTY_PADRE", "MTM", "CVA", "VR", "VR+", "ADDON", "R06"])
    file = "outputREC/R06_FX_{process_date}.xlsx"
    file = file.format(**{'process_date': processdate})
    excel = pd.ExcelWriter(file)
    df_r06.to_excel(excel, index=False, sheet_name='R06')
    factores_fx.to_excel(excel, index=False, sheet_name='Factors')
    excel.close()
    return df_r06, factores_fx


def opt_r06(processdate: date, factors, aj_cva):
    factores_opt = opt_factors(factors, processdate)
    opt_mtm = execute_query(query=MTM_OPT, parameters={'process_date': processdate})
    ops = opt_mtm.iloc[:, 0].unique()
    r06 = []
    for i in ops:
        op = factores_opt.loc[factores_opt['GID'] == int(i)]
        cliente = op['CPTY_PADRE'].iat[0]
        mtm_op = opt_mtm.loc[opt_mtm['GID'] == int(i)]
        mtm = mtm_op['MTM'].iat[0]
        cva_op = aj_cva.loc[aj_cva['CONTRATO'] == int(i)]
        try:
            aj = cva_op['Ajuste_CVA'].iat[0]
        except:
            aj = 0
        plazo = op.iloc[:, 1].unique()
        addon = 0
        for j in plazo:
            flujo = op.loc[op['PLAZO'] == j]
            factor = flujo['FACTOR_NORMATIVO'].iat[0] / 100
            amortizacion = flujo['AMORTIZACION'].iat[0]
            addon += factor * amortizacion
        vr = mtm + aj
        vr_plus = max(vr, 0)
        r06_op = vr_plus + addon
        r06.append([int(i), cliente, mtm, aj, vr, vr_plus, addon, r06_op])
    df_r06 = pd.DataFrame(r06, columns=["GID", "CPTY_PADRE", "MTM", "CVA", "VR", "VR+", "ADDON", "R06"])
    file = "outputREC/R06_OPT_{process_date}.xlsx"
    file = file.format(**{'process_date': processdate})
    excel = pd.ExcelWriter(file)
    df_r06.to_excel(excel, index=False, sheet_name='R06')
    factores_opt.to_excel(excel, index=False, sheet_name='Factors')
    excel.close()
    return df_r06, factores_opt


def all_r06(processdate: date):
    factors = normative_matrix(processdate)
    aj_cva = cva(processdate)
    r06_op, addon_opt = opt_r06(processdate, factors, aj_cva)
    r06_fx, addon_fx = fx_r06(processdate, factors, aj_cva)
    r06_swap, addon_swap = swap_r06(processdate, factors, aj_cva)
    r06 = pd.concat([r06_op, r06_fx, r06_swap])
    addon = pd.concat([addon_opt, addon_fx, addon_swap])
    file = "outputREC/R06_{process_date}.xlsx"
    file = file.format(**{'process_date': processdate})
    excel = pd.ExcelWriter(file)
    r06.to_excel(excel, index=False, sheet_name='R06')
    addon.to_excel(excel, index=False, sheet_name='ADDON')
    excel.close()
    return r06


def generate_normative(processdate: date):
    r06 = all_r06(processdate)
    counterparties_data = cptys(processdate)
    counterparties = r06.iloc[:, 1].unique()
    rec = []
    for i in counterparties:
        counterparty = r06.loc[r06["CPTY_PADRE"] == i]
        counterparty_data = counterparties_data.loc[counterparties_data["CPTY_PADRE"] == i]
        netting = counterparty_data["NETTING"].iat[0]
        garantia = counterparty_data["GARANTIA"].iat[0]
        ops = counterparty.iloc[:, 0].unique()
        sum_r06 = 0
        sum_addon = 0
        sum_vr = 0
        sum_vr_plus = 0
        for j in ops:
            op = counterparty.loc[counterparty["GID"] == j]
            sum_r06 += op["R06"].iat[0]
            sum_addon += op["ADDON"].iat[0]
            sum_vr += op["VR"].iat[0]
            sum_vr_plus += op["VR+"].iat[0]
        if netting == 'No':
            rec_op = sum_r06
        elif sum_vr_plus == 0:
            rec_op = 0.4 * sum_addon
        else:
            rec_op = max(sum_vr, 0) + sum_addon * (0.4 + 0.6 * (max(sum_vr, 0) / sum_vr_plus))

        if garantia != 0:
            rec_op_final = max(rec_op - garantia, 0)
        else:
            rec_op_final = rec_op
        rec.append([i, rec_op_final])
    df_rec = pd.DataFrame(rec, columns=["CPTY", "REC_NORMATIVO"])
    file = "outputREC/REC_{process_date}.xlsx"
    file = file.format(**{'process_date': processdate})
    excel = pd.ExcelWriter(file)
    df_rec.to_excel(excel, index=False, sheet_name='REC')
    excel.close()
