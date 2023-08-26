This data set has been collected from a custom built battery prognostics testbed at the NASA Ames Prognostics Center of Excellence (PCoE). 
Li-ion batteries were run through 3 different operational profiles (charge, discharge and Electrochemical Impedance Spectroscopy) at different temperatures. Discharges were carried out at different current load levels until the battery voltage fell to preset voltage thresholds. 
Some of these thresholds were lower than that recommended by the OEM (2.7 V) in order to induce deep discharge aging effects. 
Repeated charge and discharge cycles result in accelerated aging of the batteries. The experiments were stopped when the batteries reached the end-of-life (EOL) criteria of 30% fade in rated capacity (from 2 Ah to 1.4 Ah).

# Charge, Discharge, Impedence
## Charge profile:

- The charge profile for all battery tests seems to be identifical.
- Charging was carried out in a constant current (CC) mode at 1.5A until the battery voltage reached 4.2V and then continued in a constant voltage (CV) mode until the charge current dropped to 20mA.

### Fields

Voltage_measured: Battery terminal voltage (Volts)
Current_measured: Battery output current (Amps)
Temperature_measured: Battery temperature (degree C)
Current_charge: Current measured at charger (Amps)
Voltage_charge: Voltage measured at charger (Volts)
Time: Time vector for the cycle (secs)


## Discharge:

- Discharge profiles were different from battery to battery.
- Discharge was carried out at a constant current (CC) level of 1-4 A until the battery voltage fell to values such 2.7V, 2.5V, 2.2V and 2.5V.

### Fields

- Voltage_measured: Battery terminal voltage (Volts)
- Current_measured: Battery output current (Amps)
- Temperature_measured: Battery temperature (degree C)
- Current_load: Current measured at load (Amps)
- Voltage_load: Voltage measured at load (Volts)
- Time: Time vector for the cycle (secs)
- Capacity: Battery capacity (Ahr) for discharge till 2.7V
- Discharge profiles were different from battery to battery.
- Discharge was carried out at a constant current (CC) level of 1-4 A until the battery voltage fell to values such 2.7V, 2.5V, 2.2V and 2.5V.

## Impedance:

- Impedance measurement was carried out through an electrochemical impedance spectroscopy (EIS) frequency sweep from 0.1Hz to 5kHz.

### Fields

- Sense_current: Current in sense branch (Amps)
- Battery_current: Current in battery branch (Amps)
- Current_ratio: Ratio of the above currents
- Battery_impedance: Battery impedance (Ohms) computed from raw data
- Rectified_impedance: Calibrated and smoothed battery impedance (Ohms)
- Re: Estimated electrolyte resistance (Ohms)
- Rct: Estimated charge transfer resistance (Ohms)


The experiments were stopped when the batteries reached a given end-of-life (EOL) criteria: for example 30% fade in rated capacity (from 2Ahr to 1.4Ahr). Other stopping criteria were used such as 20% fade in rated capacity. Note that for batteries 49,50,51,52, the experiments were not stop due to battery EOL but because the software has crashed

# Intended Use
This dataset can be used for the prediction of both:

- remaining charge (for a given discharge cycle) and,
- remaining useful life (RUL).



