class Core(object):
    @classmethod
    def on_data_obj_create(cls, func, *args, **options):
        if hasattr(cls, "pre_data_obj_create"):
            cls.pre_data_obj_create(*args, **options)

        func(*args, **options)

        if hasattr(cls, "post_data_obj_create"):
            cls.post_data_obj_create(*args, **options)

    @classmethod
    def on_data_obj_modify(cls, func, *args, **options):
        if hasattr(cls, "pre_data_obj_modify"):
            cls.pre_data_obj_modify(*args, **options)

        func(*args, **options)

        if hasattr(cls, "post_data_obj_modify"):
            cls.post_data_obj_modify(*args, **options)

    @classmethod
    def on_data_obj_delete(cls, func, *args, **options):
        if hasattr(cls, "pre_data_obj_delete"):
            cls.pre_data_obj_delete(*args, **options)

        func(*args, **options)

        if hasattr(cls, "post_data_obj_delete"):
            cls.post_data_obj_delete(*args, **options)

    @classmethod
    def on_coll_create(cls, func, *args, **options):
        if hasattr(cls, "pre_coll_create"):
            cls.pre_coll_create(*args, **options)

        func(*args, **options)

        if hasattr(cls, "post_coll_create"):
            cls.post_coll_create(*args, **options)

    @classmethod
    def on_coll_modify(cls, func, *args, **options):
        if hasattr(cls, "pre_coll_modify"):
            cls.pre_coll_modify(*args, **options)

        func(*args, **options)

        if hasattr(cls, "post_coll_modify"):
            cls.post_coll_modify(*args, **options)

    @classmethod
    def on_coll_delete(cls, func, *args, **options):
        if hasattr(cls, "pre_coll_delete"):
            cls.pre_coll_delete(*args, **options)

        func(*args, **options)

        if hasattr(cls, "post_coll_delete"):
            cls.post_coll_delete(*args, **options)
