# Stanford Fast-Charging Datasets
Experimental data of lithium-ion batteries under galvanostatic discharge tests at different rates and temperatures of operation. 
Researchers from Stanford and MIT have published two relatively large datasets including cycling data of commercial 1.1Ah 18650 LFP/Graphite cells. 
These datasets are useful in particular for applying machine learning methods. 
The Cycle Life Prediction Dataset includes 135 cells cycled to their end of life and was used for developing an accurate model for cycle life prediction using the first 100 cycles data. 
In the Fast-Charging Optimization Dataset, cells were cycled 100–120 times with 224 different charging profiles. 
This data together with the prediction model based on the Cycle Life Prediction Dataset, were used to optimize the charging profile for lifetime.

## Manufacturer Specifications 

| ESS_Model_Name  | Name | Manufacturer | Nominal_Voltage_V | Nominal_Capacity_Ah | Cell_Weight_kg | Discharge_cut_off_voltage_V |
| --------------- | ---- | ------------ | ------------------| ------------------- | -------------- | --------------------------- |
NCR18650B |	NCA |	Panasonic	| 3.6	| 3.35 | 0.0475 | 2.5
INR21700-M50 | NMC	| LG Chem	| 3.63	| 4.85	| 0.06925	| 2.5
ANR26650m1-b	| LFP	| A123 | Systems	| 3.3	| 2.5	| 0.076	| 2

