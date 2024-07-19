import mgenerate from "mgeneratejs";
import fs from "fs";

function genFilterId(nTotal) {
  let min = 1;
  let filter = [];
  let template = {
    id: { $integer: { min: min, max: min + 2000 } },
  };
  while (nTotal-- > 0) {
    min = mgenerate(template).id;
    filter.push(min);
    template.id.$integer.min = min + 1;
    template.id.$integer.max = min + 2000;
  }
  return filter;
}

function genFilter() {
  const filter = genFilterId(1000);
  filter.sort(() => Math.random() - 0.5);
  const subset = filter.slice(0, 500);
  subset.sort((a, b) => a - b);

  fs.writeFileSync("./filter500.json", JSON.stringify(subset, null, 2));
  fs.writeFileSync("./filter1000.json", JSON.stringify(filter, null, 2));
}

genFilter();
