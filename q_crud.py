from db.handle_return import HandleReturn
from db.helper import dbhelper
from db.dbexecute import dbexe
from db.model_product import *
from db.model_client import *
from sqlalchemy import asc, desc
from datetime import datetime
class crud_helper(dbhelper):
    def __init__(self) -> None:
        super().__init__()
        self.Open() 
        
    def write_obj(self, table_name, obj, check_parent_exists=False):
        """Hàm write obj vô database, hỗ trợ check FK hợp lệ \n
        Hiện tại (1/12) có thể sd cho table:
            GroupChannels, Channels, Locations, ProductType, ProductsAttribute, ProductsOptionAttribute

        Args:
            table_name (str): tên table cần ghi xuống
            obj (schema): obj cần ghi xuống
            check_parent_exists (bool, optional): check FK hợp lệ. Defaults to False.

        Returns:
            HandleReturn().response()
        """
        model = self.get_model_from_table_name(table_name)
        if model:
            session = next(self.get_session())
            if check_parent_exists:
                if model == Channels:
                    parent = session.query(GroupChannels).get(obj.group_channel_ID)
                if model == Locations:
                    parent = session.query(Channels).get(obj.channel_ID)
                
                if not parent or parent.is_disable == True:
                    return HandleReturn().response(500, False, f'Tạo thất bại (FK không tồn tại/ đã disable)')

            # Thực hiện write 
            new_obj = dbexe()
            obj_id = new_obj.write_data(model(**obj.dict()))
            if obj_id:
                return HandleReturn().response(200, True, f'Tạo thành công (ID {obj_id})')
            else:
                return HandleReturn().response(500, False, f'Tạo thất bại')
        else:
            return HandleReturn().response(500, False, f'Đã xảy ra lỗi (#235)')
    
    def update_obj(self, table_name, id, obj, check_parent_exists=False):
        """Hàm update obj vô database, hỗ trợ check id và FK hợp lệ \n
        Hiện tại (1/12) có thể sd cho table:
            GroupChannels, Channels, Locations, ProductType, ProductsAttribute, ProductsOptionAttribute

        Args:
            table_name (str): tên table cần ghi xuống
            id (int): PK của row cần update 
            obj (schema): obj cần ghi xuống
            check_parent_exists (bool, optional): check FK hợp lệ. Defaults to False.

        Returns:
            HandleReturn().response()
        """
        model = self.get_model_from_table_name(table_name)
        if model:
            session = next(self.get_session())
            target_obj = session.query(model).get(id)

            if not target_obj or target_obj.is_disable == True:
                return HandleReturn().response(500, False, f'Cập nhật thất bại (ID không tồn tại/ đã disable)')
                
            if check_parent_exists:
                if model == Channels:
                    parent = session.query(GroupChannels).get(obj.group_channel_ID)
                if model == Locations:
                    parent = session.query(Channels).get(obj.channel_ID)
                
                if not parent or parent.is_disable == True:
                    return HandleReturn().response(500, False, f'Cập nhật thất bại (FK không tồn tại/ đã disable)')

            # Thực hiện update 
            try:
                session.query(model).filter(model.id == id).update(dict(obj))
                target_obj.update_date = datetime.now()
                # C2: setattr(model, 'column_name', 'value')
                session.commit()
                return HandleReturn().response(200, True, f'Cập nhật thành công (ID {1})')
            except:
                return HandleReturn().response(500, False, f'Cập nhật thất bại')
        else:
            return HandleReturn().response(500, False, f'Đã xảy ra lỗi (#235)')
    
    def disable_obj(self, table_name, id):
        model = self.get_model_from_table_name(table_name)
        if model:
            session = next(self.get_session())
            obj = session.query(model).get(id)
            if obj and obj.is_disable == False:
                obj.is_disable = True
                obj.update_date = datetime.now()
                session.commit()
                return HandleReturn().response(200, True, f'Xóa thành công')
            else:
                return HandleReturn().response(500, False, f'Xoá thất bại')
        else:
            return HandleReturn().response(500, False, f'Đã xảy ra lỗi (#235)')
        
    
    def paginate(self, table_name, params):
        model = self.get_model_from_table_name(table_name)
        if model:
            session = next(self.get_session())
            query = session.query(model)
            total = query.count()
            if params.order:
                direction = desc if params.order == 'desc' else asc
                query = query.order_by(direction(getattr(model, params.sort_by)))

            data = query.limit(params.page_size).offset(params.page_size * (params.page-1)).all()

            return {
                'params': params,
                'total': total,
                'data': data
            }
        else:
            return HandleReturn().response(500, False, f'Đã xảy ra lỗi (#235)')
            
            
    def get_model_from_table_name(self, model):
        """
            Input: 
                + model (Class)
            Return: Class tương ứng
        """
        # all_tables = Base.metadata.tables.keys()
        # for column in list(model.__table__._columns):
            #     print(column.name)
                # if column.name == 'name':
                    
            # print(model.__table__.c)
        all_models = [GroupChannels, Channels, Locations, ProductsType, ProductsAttribute, ProductsOptionAttribute, Clients]
        if model in all_models:
            return model
        # for model in all_models:
        #     if model.__tablename__ == table_name:
        #         return model
        return False        
    
    def get_session(self):
        session = self._Session()
        try:
            yield session
        finally:
            session.close()
            
    