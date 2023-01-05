import sys
import re
from concurrent.futures import ThreadPoolExecutor
import toml

from runtype import dataclass

try:
    import textual
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "Error: Cannot find the TUI library 'textual'. Please install it using `pip install sqeleton[tui]`"
    )

from textual.app import App, ComposeResult
from textual.containers import Vertical, Container, Horizontal
from textual.widgets import Header, Footer, Input, Label, Button, Static, ListView, ListItem

from textual_select import Select

from .databases._connect import DATABASE_BY_SCHEME
from . import connect


class ContentSwapper(Container):
    def __init__(self, *initial_widgets, **kw):
        self.container = Container(*initial_widgets)
        super().__init__(**kw)

    def compose(self):
        yield self.container

    def new_content(self, *new_widgets):
        for c in self.container.children:
            c.remove()

        self.container.mount(*new_widgets)


def test_connect(connect_dict):
    conn = connect(connect_dict)
    assert conn.query("select 1+1", int) == 2


@dataclass
class Config:
    config: dict

    @property
    def databases(self):
        if "database" not in self.config:
            self.config["database"] = {}
        assert isinstance(self.config["database"], dict)
        return self.config["database"]

    def add_database(self, name: str, db: dict):
        assert isinstance(db, dict)
        self.config["database"][name] = db

    def remove_database(self, name):
        del self.config["database"][name]


class EditConnection(Vertical):
    def __init__(self, db_name: str, config: Config) -> None:
        self._db_name = db_name
        self._config = config
        super().__init__()

    def compose(self):
        self.params_container = ContentSwapper(id="params")
        self.driver_select = Select(
            placeholder="Select a database driver",
            search=True,
            items=[{"value": k, "text": k} for k in DATABASE_BY_SCHEME],
            list_mount="#driver_container",
            value=self._config.databases.get(self._db_name, {}).get("driver"),
            id="driver",
        )

        yield Vertical(
            Label("Connection name:"),
            Input(id="conn_name", value=self._db_name, classes="margin-bottom-1"),
            Label("Database Driver:"),
            self.driver_select,
            self.params_container,
            id="driver_container",
        )

        self.create_params()

    def create_params(self):
        driver = self.driver_select.value
        if not driver:
            return
        db_config = self._config.databases.get(self._db_name, {})

        db_cls = DATABASE_BY_SCHEME[driver]
        # Filter out repetitions, but keep order
        base_params = re.findall("<(.*?)>", db_cls.CONNECT_URI_HELP)
        params = dict.fromkeys([p.lower() for p in base_params] + [k for k in db_config if k != "driver"])

        widgets = []
        for p in params:
            label = p
            if p in ("user", "pass", "port", "dbname"):
                label += " (optional)"
            widgets.append(Label(f"{label}:"))
            p = p.lower()
            widgets.append(Input(id="input_" + p, name=p, classes="param", value=db_config.get(p)))

        self.params_container.new_content(
            Vertical(
                *widgets,
                Horizontal(
                    Button("Test & Save Connection", id="test_and_save"),
                    Button("Save Connection Without Testing", id="save_without_test"),
                ),
                Vertical(id="result"),
            )
        )

    def on_select_changed(self, event: Select.Changed) -> None:
        driver = str(event.value)
        self.driver_select.text = driver
        self.driver_select.value = driver
        self.create_params()

    def _get_connect_dict(self):
        connect_dict = {"driver": self.driver_select.value}
        for p in self.query(".param"):
            if p.value:
                connect_dict[p.name] = p.value

        return connect_dict

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id
        if button_id == "test_and_save":
            connect_dict = self._get_connect_dict()

            result_container = self.query_one("#result")
            result_container.mount(Label(f"Trying to connect to {connect_dict}"))

            try:
                test_connect(connect_dict)
            except Exception as e:
                error = str(e)
                result_container.mount(Label(f"[red]Error: {error}[/red]"))
            else:
                result_container.mount(Label(f"[green]Success![green]"))
                self.save(connect_dict)

        elif button_id == "save_without_test":
            connect_dict = self._get_connect_dict()
            self.save(connect_dict)

    def save(self, connect_dict):
        assert isinstance(connect_dict, dict)
        result_container = self.query_one("#result")
        result_container.mount(Label(f"Database saved"))

        name = self.query_one("#conn_name").value
        self._config.add_database(name, connect_dict)
        self.app.config_changed()


class ListOfConnections(Vertical):
    def __init__(self, config: Config, **kw):
        self.config = config
        super().__init__(**kw)

    def compose(self) -> ComposeResult:
        list_items = [
            ListItem(Label(name, id="list_label_" + name), name=name) for name in self.config.databases.keys()
        ]

        self.lv = lv = ListView(*list_items, id="connection_list")
        yield lv

        lv.focus()

    def on_list_view_highlighted(self, h: ListView.Highlighted):
        name = h.item.name
        self.app.query_one("#edit_container").new_content(EditConnection(name, self.config))


class ConnectionEditor(App):
    CSS = """
    #conn_list {
        display: block;
        dock: left;
        height: 100%;
        max-width: 30%;
        margin: 1
    }

    #edit {
        margin: 1
    }

    #test_and_save {
        margin: 1
    }
    #save_without_test {
        margin: 1
    }

    .margin-bottom-1 {
        margin-bottom: 1
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("d", "toggle_dark", "Toggle dark mode"),
        ("insert", "add_conn", "Add new connection"),
        ("delete", "del_conn", "Delete selected connection"),
        ("t", "test_conn", "Test selected connection"),
        ("a", "test_all_conns", "Test all connections"),
    ]

    def run(self, toml_path, **kw):
        self.toml_path = toml_path
        try:
            with open(self.toml_path) as f:
                self.config = Config(toml.load(f))
        except FileNotFoundError:
            self.config = Config({})

        return super().run(**kw)

    def config_changed(self):
        self.list_swapper.new_content(ListOfConnections(self.config))
        with open(self.toml_path, "w") as f:
            toml.dump(self.config.config, f)

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        self.list_swapper = ContentSwapper(ListOfConnections(self.config), id="conn_list")
        self.edit_swapper = ContentSwapper(id="edit_container")
        self.edit_swapper.new_content(EditConnection("New", self.config))

        yield Header()
        yield Container(self.list_swapper, self.edit_swapper)
        yield Footer()

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.dark = not self.dark

    def action_test_all_conns(self):
        return self.action_quit()

    def action_add_conn(self):
        self.edit_swapper.new_content(EditConnection("New", self.config))

    def _selected_connection(self):
        connection_list: ListView = self.query_one("#connection_list")
        return connection_list.children[connection_list.index]

    def action_del_conn(self):
        name = self._selected_connection().name
        self.config.remove_database(name)
        self.config_changed()

    def _test_existing_db(self, name):
        label: Label = self.query_one("#list_label_" + name)
        label.update(f"{name}🔃")
        connect_dict = self.config.databases[name]
        try:
            test_connect(connect_dict)
        except Exception as e:
            label.update(f"{name}❌ {str(e)[:16]}")
        else:
            label.update(f"{name}✅")

    def action_test_conn(self):
        name = self._selected_connection().name
        t = ThreadPoolExecutor()
        t.submit(self._test_existing_db, name)

    def action_test_all_conns(self):
        t = ThreadPoolExecutor()
        for name in self.config.databases:
            t.submit(self._test_existing_db, name)


def main(toml_path: str):
    app = ConnectionEditor()
    app.run(toml_path)


if __name__ == "__main__":
    main(sys.argv[1])
