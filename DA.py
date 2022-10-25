import pandas as pd
import numpy as np
#Constants for proptables
FLOW = "Flows"
SYMP = "Symptoms"


path = "C:/Users/61435/OneDrive - PHM Technology/PHM Tech/Research/Testability/Testability Mini Spec/"
print(path)
filename = "ProptableMini_Exclusions"

class PropagationTable:
    def __init__(self, name, pt , sensors, parent):
        parent.propagation_tables[name] = self
        self.name = name
        self.pt = pt
        self.sensors = sensors
        self.length = self.pt.shape[0]
        self.coverage = None
        self.detection = None
        #self.amgiguety_groups = None
        self.unique = ['Diagnostic Group', 'Component', 'Flow Property', 'Failure']
        self.pt['group'] = ""

    def do_calcs(self):
        self.get_failure_mode_list()
        self.calc_detection()
        self.calc_coverage()

    def get_failure_mode_list(self):
        """
        Get list of unique failure modes
        :return:
        """
        self.pt["Failure_Mode"] = self.pt[self.unique[1]] + self.pt[self.unique[2]] + self.pt[self.unique[3]]

    def filter_pt(self,sensors = None):
        if sensors is not None:
            return self.pt[sensors]
        else:
            return self.pt

    def calc_detection(self):
        """
        A failure mode is detectable if any of the sensors in its syndrome are not zero
        :param sensors:
        :return:
        """

        count = 0
        for index, row in self.pt.iterrows():
            # If the sum absolute value of the propagation table row is 0, it is not detectable
            if self.pt.loc[index, self.sensors].abs().sum() == 0:
                count += 1

        self.detection = (1 - count / self.length) * 100

    def calc_coverage(self):
        """
        A failure mode is covered if it has a unique syndrome in the propagation table
        :param sensors:
        :return:
        """
        print(self.name)
        #pt = self.pt
        unique = {}
        for index, row in self.pt.iterrows():
            key = str(np.array(row[self.sensors]))
            if unique.get(key) is None:
                unique[key]= [index]
            else:
                unique[key].append(index)
        i = 0
        count = 0
        print(f"    {unique}")
        for key in unique:
            if len(unique[key]) == 1:
                count += 1
                for index in unique[key]:
                    print(f"        update {i}")
                    self.pt.loc[index, 'group'] = "F " + str(i)

            else:
                for index in unique[key]:
                    print(f"        update {i}")
                    self.pt.loc[index, 'group'] = "G " + str(i)
            i += 1

        self.coverage = (count/self.length)*100

    def save_stats(self, writer):
        # Create pt
        cols = self.unique + self.sensors + ['group']
        pt = self.pt[cols]
        pt.to_excel(writer, self.name)

        # Create Stats df:
        dic = {}
        dic["Coverage"] = self.coverage
        dic["Detection"] = self.detection
        dic["Num_sensor"] = len(self.sensors)
        df = pd.DataFrame(dic, index=[0])
        df.to_excel(writer, self.name + "_stats")

class Diag_Analysis:
    def __init__(self, filename, name):
        self.filename = filename
        self.filename_xlsx = filename + ".xlsx"
        self.name = name
        self.labels = ['Diagnostic Group', 'Component', 'Flow Property', 'Failure']

        # get PT and Merge
        pt_flow = pd.read_excel(self.filename_xlsx, sheet_name=FLOW).replace(np.nan, 0)
        pt_symp = pd.read_excel(self.filename_xlsx, sheet_name=SYMP).replace(np.nan, 0)
        assert pt_flow['Diagnostic Group'].equals(pt_symp['Diagnostic Group'])
        assert pt_flow['Component'].equals(pt_symp['Component'])
        assert pt_flow['Flow Property'].equals(pt_symp['Flow Property'])
        assert pt_flow['Failure'].equals(pt_symp['Failure'])

        # Get sensor locations and merge
        symp_cols = list(pt_symp.columns)
        flow_cols = list(pt_flow.columns)
        for el in self.labels: symp_cols.remove(el)
        for el in self.labels: flow_cols.remove(el)
        both_cols = flow_cols + symp_cols

        pt_both = pd.concat([pt_flow, pt_symp[symp_cols]], axis=1)
        # pt_both.reset_index(inplace=True, drop=True)
        assert pt_both['Diagnostic Group'].equals(pt_symp['Diagnostic Group'])
        assert pt_both['Component'].equals(pt_symp['Component'])
        assert pt_both['Flow Property'].equals(pt_symp['Flow Property'])
        assert pt_both['Failure'].equals(pt_symp['Failure'])

        # Create PT objects
        self.propagation_tables = {}
        self.symptoms = PropagationTable("symptoms", pt_symp, symp_cols, parent=self)
        self.flow = PropagationTable("flows", pt_flow, flow_cols, parent=self)
        self.both = PropagationTable("both", pt_both, both_cols, parent=self)

        # self.symptoms.do_calcs()
        # self.flow.do_calcs()
        # self.both.do_calcs()
        print("here")

    def add_pt(self, name, sensors):
        PropagationTable(name, self.both.pt.copy(), sensors, parent=self)

    def calc_stats(self):
        for pt in self.propagation_tables:
            self.propagation_tables[pt].do_calcs()

    def save_stats(self):
        with pd.ExcelWriter(self.filename+"_out"+".xlsx") as writer:
            for pt in self.propagation_tables:
                self.propagation_tables[pt].save_stats(writer)

    def include(self):
        pass


base_da = Diag_Analysis(path + filename, "Base")
# S1 = ['C1 (Pneumatic - Mass flow rate)', 'C2 (Pneumatic - Mass flow rate)', 'Combustion System (Continuous - Data)', 'Engine (Discrete - Data)', 'Engine (Discrete - Data).1', 'Other G1']
# A3_S1_tD = ['C1 (Pneumatic - Mass flow rate)','C1 (Pneumatic - Mass flow rate).1','C2 (Pneumatic - Mass flow rate)','CV1 (Liquid - Flow rate)','CV1 (Liquid - Contamination)','Combustion System (Mechanical - rotational - Angular velocity)','Combustion System (Pneumatic - Mass flow rate)','Combustion System (Mechanical - rotational - Angular velocity)','O Pump (Gas - Static pressure)','Engine (Discrete - Data)','Engine (Discrete - Data).1','Engine (Electrical - Voltage)','CV2 (Gas - Mass flow rate)', 'Exhaust Nozzle (Continuous - Data)', 'Exhaust Nozzle (Pneumatic - Mass flow rate)', 'Vibration G1',  'Change in Behaviour']
# A3_S1_tD.sort()
# A4_S3 = ['Exhaust Nozzle (Pneumatic - Mass flow rate)', 'Vibration G1', 'C1 (Pneumatic - Mass flow rate).1']
# A4_S6 = ['O Pump (Gas - Static pressure)', 'Vibration G1', 'C1 (Pneumatic - Mass flow rate).1']

# base_da.add_pt('S1', S1)
# base_da.add_pt('A3 - S1 - test Diagnostic', A3_S1_tD)
# base_da.add_pt('A4 - S3 - John Yoo', A4_S3)
# base_da.add_pt('A4 - S6 - John Yoo', A4_S6)
# base_da.add_pt('S1', S1)

base_da.calc_stats()
base_da.save_stats()

