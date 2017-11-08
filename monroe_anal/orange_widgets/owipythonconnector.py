import os
from glob import glob

import numpy as np
import pandas as pd

from AnyQt.QtWidgets import QApplication
from AnyQt.QtCore import Qt, QFileSystemWatcher
from pandas.api.types import is_categorical_dtype, is_object_dtype, is_datetime64_any_dtype, is_numeric_dtype

from Orange.data import (
    Table, Domain, DiscreteVariable, StringVariable,
    TimeVariable,
    ContinuousVariable,
)
from Orange.widgets import widget, settings, gui

from monroe_anal.ipython_connector import IPythonStore


STORE = IPythonStore()


def _table_from_frame(df):

    def _is_discrete(s):
        return (is_categorical_dtype(s) or
                is_object_dtype(s) and s.nunique() < s.size ** .5)

    def _is_datetime(s):
        if is_datetime64_any_dtype(s):
            return True
        try:
            if is_object_dtype(s):
                pd.to_datetime(s, infer_datetime_format=True)
                return True
        except Exception:
            pass
        return False

    attrs, metas = [], []
    X, M = [], []

    for name, s in df.items():
        name = str(name)
        if _is_discrete(s):
            discrete = s.astype('category').cat
            attrs.append(DiscreteVariable(name, discrete.categories.astype(str).tolist()))
            X.append(discrete.codes.replace(-1, np.nan).values)
        elif _is_datetime(s):
            tvar = TimeVariable(name)
            attrs.append(tvar)
            s = pd.to_datetime(s, infer_datetime_format=True)
            X.append(s.astype('str').map(tvar.parse).values)
        elif is_numeric_dtype(s):
            attrs.append(ContinuousVariable(name))
            X.append(s.values)
        else:
            metas.append(StringVariable(name))
            M.append(s.values.astype(object))

    MAX_LENGTH = max(len(X[0]) if X else 0,
                             len(M[0]) if M else 0)
    ZERO_CONSTANT = 0
    return Table.from_numpy(Domain(attrs, None, metas),
                            np.column_stack(X) if X else np.empty((MAX_LENGTH,
                                                                   ZERO_CONSTANT)),
                            None, np.column_stack(M) if M else None)


def _table_from_numpy(x):
    def _to2d(x):
        if x.ndim <= 1:
            return np.c_[x]
        if x.ndim == 2:
            return x
        return None
        # When the shitty internals get fixed, this below will work
        # 2d array of (n-2)d-list fields
        x2d = np.empty(x.shape[:2], dtype=object)
        x2d[:] = x.tolist()
        return x2d

    x = _to2d(x)
    # 2d or str arrays etc. not supported
    if x is None or not is_numeric_dtype(x):
        return None
    return Table.from_numpy(None, x)


# Mapping of Jupyter types to callable conversions to Orange Table
VALID_DATA_TYPES = {
    pd.DataFrame: _table_from_frame,
    pd.Series: lambda s: _table_from_frame(s.to_frame()),
    np.ndarray: _table_from_numpy,
}


class OWIPythonConnector(widget.OWWidget):
    name = 'IPython Connector'
    description = 'Import objects stored with IPython/Jupyter %store magic command.'
    icon = 'icons/IPythonConnector.svg'
    priority = 100

    class Outputs(widget.OWWidget.Outputs):
        data = widget.Output('Data', Table)
        object = widget.Output('Raw Object', object)

    want_main_area = False
    resizing_enabled = False

    selected = settings.Setting('')
    auto_commit = settings.Setting(True)

    def __init__(self):
        self.output_obj = None
        self.output_data = None

        self.combo = gui.comboBox(
            self.controlArea, self, 'selected', box='Stored Jupyter Object',
            sendSelectedValue=True, callback=self.output)
        self.combo.setToolTip('Variables, stored with %store magic command '
                              'in IPython/Jupyter shell/notebook will be '
                              'available here.')

        gui.auto_commit(self.buttonsArea, self, 'auto_commit', label='Send')

        self.watcher = QFileSystemWatcher([STORE.root] + self._glob_files(), parent=self,
                                          directoryChanged=self.on_dir_changed,
                                          fileChanged=self.on_file_changed)
        assert self.watcher.directories() == [STORE.root]

        self.update_combo()
        self.output()

    def _glob_files(self):
        return glob(os.path.join(STORE.root, '*'))

    def update_combo(self):
        selected = self.selected

        def type_name(x):
            module = type(x).__module__
            return (module + '.' if module and module != str.__module__ else '') + type(x).__qualname__

        key_type = ((type_name(v), k) for k, v in STORE.items())
        items = ['{}  ({})'.format(key, type)
                 for type, key in sorted(key_type)]

        self.combo.blockSignals(True)

        self.combo.clear()
        self.combo.addItems(items)
        self.selected = selected if selected in items else ''

        self.combo.blockSignals(False)

    def on_dir_changed(self, path):
        watched = set(self.watcher.files())
        dir_contents = set(self._glob_files())

        files_added = dir_contents - watched
        files_removed = watched - dir_contents

        if files_added:
            self.watcher.addPaths(list(files_added))
        if files_removed:
            self.watcher.removePaths(list(files_removed))

        self.update_combo()

    def on_file_changed(self, path):
        key = os.path.basename(path)
        selected = self.selected and self.selected.split()[0]
        if selected == key:
            self.output()

    def output(self):
        key = self.selected and self.selected.split()[0]
        output_obj = STORE.get(key)
        output_data = next((func(output_obj)
                            for type, func in VALID_DATA_TYPES.items()
                            if isinstance(output_obj, type)), None)
        self.output_obj = output_obj
        self.output_data = output_data
        self.commit()

    def commit(self):
        self.Outputs.object.send(self.output_obj)
        self.Outputs.data.send(self.output_data)


if __name__ == "__main__":
    a = QApplication([])
    ow = OWIPythonConnector()
    ow.show()
    a.exec()
