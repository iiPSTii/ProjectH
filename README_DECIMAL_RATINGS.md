# Supporto per Valutazioni Decimali in FindMyCure Italia

Questo documento descrive come il sistema FindMyCure Italia supporta le valutazioni con precisione decimale (ad esempio 3.7, 4.2, ecc.) per le strutture sanitarie.

## Panoramica

Il sistema è stato progettato fin dall'inizio per supportare valutazioni con precisione decimale. Le caratteristiche principali includono:

1. **Supporto Database**: La colonna `quality_rating` nella tabella `facility_specialty` è di tipo FLOAT/Double precision
2. **Modelli Python**: Il modello `FacilitySpecialty` utilizza `db.Float` per il campo quality_rating
3. **Visualizzazione**: Il frontend mostra correttamente le mezze stelle e percentuali precise
4. **Importazione**: Gli script supportano l'importazione di valori decimali da CSV

## Strumenti Inclusi

Per facilitare la gestione delle valutazioni decimali, sono disponibili i seguenti strumenti:

### 1. Import Decimal Ratings

Lo script `import_decimal_ratings.py` è progettato specificamente per importare rating decimali:

```bash
python import_decimal_ratings.py <file_csv> [--batch-size 20] [--skip-backup]
```

**Caratteristiche**:
- Supporto completo per valori decimali (es. 3.7, 4.2)
- Backup automatico del database prima dell'importazione
- Elaborazione a batch per evitare timeout
- Report dettagliato delle modifiche

### 2. Prepare Import CSV

Lo script `prepare_import_csv.py` converte un file CSV con valutazioni intere in un file CSV con valutazioni decimali:

```bash
python prepare_import_csv.py <file_csv_input> <file_csv_output>
```

**Caratteristiche**:
- Converte valutazioni intere in decimali in modo realistico
- Mantiene l'integrità dei dati originali
- Genera valori decimali plausibili

### 3. Verify Decimal Ratings

Lo script `verify_decimal_ratings.py` verifica che i rating decimali siano correttamente supportati:

```bash
python verify_decimal_ratings.py [--detailed]
```

**Caratteristiche**:
- Verifica il supporto per valori decimali
- Fornisce statistiche sui rating
- Mostra esempi di rating decimali

## Progettazione del Sistema

### Database

La colonna `quality_rating` è definita come:

```python
quality_rating = db.Column(db.Float, default=None)
```

Questo permette di memorizzare con precisione valori come 3.7, 4.2, ecc.

### Frontend

Il frontend è progettato per mostrare:

1. **Stelle intere**: Per la parte intera del rating (es. 3 stelle per 3.7)
2. **Mezze stelle**: Per rating con parte decimale ≥ 0.5 (es. 3.5, 4.5)
3. **Percentuali precise**: I progressbar mostrano la percentuale esatta (es. 74% per 3.7/5)

## Casi d'Uso

Le valutazioni decimali offrono diversi vantaggi:

1. **Maggiore Precisione**: Rappresentazione più accurata della qualità delle strutture
2. **Differenziazione**: Migliore distinzione tra strutture con qualità simile
3. **Visualizzazione Avanzata**: Esperienza utente migliorata con mezze stelle e percentuali precise

## Formato CSV Atteso

Per utilizzare lo script di importazione, il file CSV deve avere il seguente formato:

```
Name of the facility,Cardiologia,Ortopedia,Oncologia,...
Ospedale San Raffaele,4.3,3.7,4.8,...
Ospedale Niguarda,4.1,4.5,3.9,...
```

La prima colonna deve essere `Name of the facility`, seguita dai nomi delle specialità. I valori possono utilizzare sia il punto (.) che la virgola (,) come separatore decimale.

## Limiti e Considerazioni

- I valori vengono automaticamente limitati all'intervallo 1.0-5.0
- Valori non numerici vengono ignorati
- I valori sono arrotondati a una cifra decimale per coerenza visiva