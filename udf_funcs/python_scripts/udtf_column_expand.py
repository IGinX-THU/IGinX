from iginx_udf import UDTFinDF


class UDFColumnExpand(UDTFinDF):
    def eval(self, data):
        data = data.drop(columns=['key'])
        ori_cols = list(data)
        for i, col_name in enumerate(ori_cols):
            new_pos = 3 * i + 1
            add_column = data[col_name] + 1.5
            mul_column = data[col_name] * 2
            data.rename(columns={col_name: f'{self.udf_name}({col_name})'}, inplace=True)
            data.insert(loc=new_pos, column=f'{self.udf_name}({col_name}+1.5)', value=add_column)
            data.insert(loc=new_pos+1, column=f'{self.udf_name}({col_name}*2)', value=mul_column)
        return data
