import importlib.util
spec = importlib.util.spec_from_file_location("Peripheral Clinics.PY_PBI022_FACT_PeripheralClinics",
                                              'G:/PerfInfo/Performance Management/OR Team/BI Reports/Peripheral Clinics/PY_PBI022_FACT_PeripheralClinics.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

clinic_results, outpatient_results, mapping_results = module.main()