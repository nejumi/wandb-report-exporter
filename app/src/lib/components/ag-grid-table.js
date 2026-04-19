import {AllCommunityModule, ModuleRegistry, createGrid} from "ag-grid-community";

ModuleRegistry.registerModules([AllCommunityModule]);

export function renderGrid({container, columnDefs, rowData, height, getRowHeight}) {
  container.className = "ag-theme-quartz table-shell surface";
  container.style.height = `${height || 520}px`;
  createGrid(container, {
    columnDefs,
    rowData,
    defaultColDef: {
      sortable: true,
      filter: true,
      resizable: true,
      minWidth: 120,
      floatingFilter: true
    },
    animateRows: true,
    pagination: true,
    paginationPageSize: 25,
    getRowHeight,
    suppressColumnVirtualisation: false,
    rowSelection: {
      mode: "singleRow",
      enableClickSelection: true
    }
  });
}
