CURR_PAIR = """
    select distinct 
    ACT.moneda as moneda_activa,
    PAS.moneda as moneda_pasiva
    from (
        select distinct numerooperacion,moneda
        from banco.dbo.CarteraDerivados 
        where fechaproceso = '{process_date}' and tipoflujo = 'ACT'
        ) ACT
    left join
        (select distinct numerooperacion,moneda
        from banco.dbo.CarteraDerivados 
        where fechaproceso = '{process_date}' and tipoflujo = 'PAS'
        ) PAS
    on ACT.numerooperacion = PAS.numerooperacion
    """

CPTY = """
    select 
    distinct 
    CPTY.NOMBRE as NOMBRE,
    CPTY_PADRE.NOMBRE as NOMBRE_PADRE,
    CPTY_PADRE.NETTING,
    CPTY_PADRE.BILATERAL,
    CPTY_PADRE.GARANTIA,
    CPTY_PADRE.MONEDA_GARANTIA
    from 
    banco.dbo.MUREX_REC_CONTRAPARTES CPTY
    left join banco.dbo.MUREX_REC_CONTRAPARTES CPTY_PADRE
    on CPTY.RUT_PADRE = CPTY_PADRE.RUT_CLIENTE
    where 
    CPTY.FECHA_PROCESO = '{process_date}'
    and CPTY_PADRE.FECHA_PROCESO = '{process_date}'
    """

AJUSTE_CVA = """
    SELECT 
    cast(CONTRATO as int) as CONTRATO,
    SUM(Ajuste_CVA) as Ajuste_CVA
    FROM 
    BANCO.dbo.Murex_Reporte_DIARIO_REC 
    WHERE 
    FECHA_PROCESO = '{process_date}'
    group by
    contrato
    """

DETALLE_SWAP = """
    select 
    *
    from
    (
        select 
        cast(ACT.GID as int) as numerooperacion,
        nombre_cliente,
        ACT.instrumento as instrumento,
        ACT.moneda as moneda_activa,
        PAS.moneda as moneda_pasiva,
        plazo_residual as plazo,
        case when act.moneda = 'CLP' then case when MAX_PLAZO.plazo = act.plazo_residual and amortizacion_acum = 0 then saldo_residual else Amortizacion end else case when MAX_PLAZO.plazo = act.plazo_residual and amortizacion_acum = 0 then saldo_residual else Amortizacion end * TC.MID_DSC end as amortizacion
    
        from (
            select 
            GID,
            instrumento,
            moneda,
            plazo_residual,
            amortizacion,
            saldo_residual,
            nombre_cliente
            from banco.dbo.MUREX_GRF_CARTERA_SWAP
            where fecha_proceso = '{process_date}' and tipo_flujo = 'ACT'
            and nombre_cliente <> 'BANCO ITAU CHILE'
        ) ACT left join (
            select distinct 
            GID,
            moneda
            from banco.dbo.MUREX_GRF_CARTERA_SWAP
            where fecha_proceso = '{process_date}' and tipo_flujo = 'PAS'
            and nombre_cliente <> 'BANCO ITAU CHILE'
        ) PAS on act.GID = pas.GID
        left join 
        (
            select MID_DSC,Primera_Moneda 
            from banco.[dbo].[MUREX_EOD_PARIDADES] 
            where FECHA_PROCESO = '{process_date}' and Segunda_Moneda = 'CLP'
        ) TC  on ACT.moneda = tc.Primera_Moneda
        left join
        (
            select 
            GID,
            max(plazo_residual) as plazo,
            sum(amortizacion) as amortizacion_acum
            from banco.dbo.MUREX_GRF_CARTERA_SWAP
            where fecha_proceso = '{process_date}' and tipo_flujo = 'ACT'
            and nombre_cliente <> 'BANCO ITAU CHILE'
            group by gid
        ) MAX_PLAZO
        on act.gid = MAX_PLAZO.GID
    ) A
    where amortizacion <> 0
    order by cast(numerooperacion as int), plazo
    """

MTM_SWAP = """
    select 
    cast(GID as int) as GID,
    sum(flujo_descontado) as MTM 
    from 
    banco.dbo.MUREX_GRF_CARTERA_SWAP 
    where 
    fecha_proceso = '{process_date}' 
    and nombre_cliente <> 'BANCO ITAU CHILE' 
    group by 
    GID
    """

DETALLE_FX = """
            select 
            cast(ACT.GID as int) as numerooperacion,
            [Nombre Cliente] as nombre_cliente,
            TP_STRTGY as instrumento,
            ACT.moneda as moneda_activa,
            PAS.moneda as moneda_pasiva,
            plazo,
            case when act.moneda = 'CLP' then [Saldo Residual] else [Saldo Residual] * TC.MID_DSC end as amortizacion
            from (
                select 
                [Numero Operacion] as GID,
                TP_STRTGY,
                moneda,
                Plazo,
                [Saldo Residual],
                [Nombre Cliente]
                from banco.dbo.Murex_GRF_Cartera_Spot_FWD
                where [Fecha Proceso] = '{process_date}' and [Tipo Flujo] = 'Act'
                and [Fecha Vencimiento] > '{process_date}'
                and [Nombre Cliente] <> 'BANCO ITAU CHILE'
            ) ACT left join (
                select distinct 
                [Numero Operacion] as GID,
                moneda
                from banco.dbo.Murex_GRF_Cartera_Spot_FWD
                where [Fecha Proceso] = '{process_date}' and [Tipo Flujo] = 'Pas'
                and [Fecha Vencimiento] > '{process_date}'
                and [Nombre Cliente] <> 'BANCO ITAU CHILE'
            ) PAS on act.GID = pas.GID
            left join 
            (
                select MID_DSC,Primera_Moneda 
                from banco.[dbo].[MUREX_EOD_PARIDADES] 
                where FECHA_PROCESO = '{process_date}' and Segunda_Moneda = 'CLP'
            ) TC  on ACT.moneda = tc.Primera_Moneda
            order by cast(ACT.GID as int), plazo
            """

MTM_FX = """
    select 
    cast([Numero Operacion] as int) as GID,
    SUM(IIF(cast(isnull([Fecha Fijacion Indice],dateadd(day,1,'{process_date}')) as date) > cast('{process_date}' as date),[Mtm En Pesos],0)) as MTM 
    from 
    banco.dbo.Murex_GRF_Cartera_Spot_FWD 
    where 
    [Fecha Proceso] = '{process_date}' 
    and [Nombre Cliente] <> 'BANCO ITAU CHILE' 
    and [Fecha Vencimiento] > '{process_date}'
    group by 
    cast([Numero Operacion] as int)
    """

DETALLE_OPT = """
            select 
            cast(numerooperacion as int) as numerooperacion,
            [Nombre Cliente] as nombre_cliente,
            tasanombre as instrumento,
            moneda as moneda_activa,
            IIF(moneda <> 'CLP','CLP','USD') as moneda_pasiva,
            plazo,
            amortizacion
            from (
                select 
                [numero operacion] as numerooperacion,
                typology as tasanombre,
                [moneda subyacente] as moneda,
                cast([fecha vencimiento]-'{process_date}' as int) as plazo,
                sum([monto subyacente]) as amortizacion,
                sum([mtm en pesos]) as mtm
                from Banco.dbo.MUREX_GRF_CARTERA_DETALLE_OPCIONES 
                where [fecha proceso] = '{process_date}'
                group by 
                [numero operacion],
                typology,
                [moneda subyacente],
                [fecha vencimiento]) A
            left join (
                select 
                [Numero Operacion],
                max([Nombre Cliente]) as [nombre cliente]
                from 
                banco.[dbo].[Murex_GRF_Cartera_Cabecera_Opciones]
                where [fecha proceso] = '{process_date}'
                group by [Numero Operacion]
            ) B on a.numerooperacion = b.[Numero Operacion]
            """

MTM_OPT = """
    select 
    cast([Numero Operacion] as int) as GID,
    SUM([Mtm En Pesos]) as MTM 
    from 
    Banco.dbo.MUREX_GRF_CARTERA_DETALLE_OPCIONES 
    where 
    [Fecha Proceso] = '{process_date}' 
    group by 
    cast([Numero Operacion] as int)
    """
