import importlib.util
spec = importlib.util.spec_from_file_location("Peripheral Clinics.FullCode",
                                              'G:/PerfInfo/Performance Management/OR Team/BI Reports/Peripheral Clinics/FullCode.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

clinic_results, outpatient_results, mapping_results = module.main()