const products = document.querySelectorAll(".box");
const currentPath = window.location.pathname;
const path = currentPath.split("/")[1].split(".")[0];

document.querySelectorAll(".option1").forEach((link) => {
  link.addEventListener("click", function (e) {
    e.preventDefault();
  });
});

document.querySelectorAll(".option2").forEach((link) => {
  link.addEventListener("click", function (e) {
    e.preventDefault();
  });
});

products.forEach((product, index) => {
  console.log(product);
  const productName = product.querySelector(".detail-box h5").innerText;
  const productId = index + 1;
  const productPrice = product
    .querySelector(".detail-box h6")
    .innerText.slice(1);

  // Add hover event listener
  product.addEventListener("mouseenter", (event) => {
    // logAction(productName, productId, productPrice, "hover", path);
  });

  // Add to Cart event listener
  const addToCartButton = product.querySelector(".option1");
  addToCartButton.addEventListener("click", async (event) => {
    event.preventDefault();
    await logAction(productName, productId, productPrice, "add_to_cart", path);
  });

  // Buy Now event listener
  const buyButton = product.querySelector(".option2");
  buyButton.addEventListener("click", async (event) => {
    event.preventDefault();
    await logAction(productName, productId, productPrice, "buy", path);
    return false;
  });
});

async function logAction(
  productName,
  productId,
  productPrice,
  actionName,
  currentPath
) {
  const logData = {
    productName,
    productId,
    productPrice,
    actionName,
    currentPath,
    timestamp: new Date().toISOString(),
  };

  try {
    const response = await fetch("http://localhost:3000/log", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(logData),
    });
  } catch (error) {
    console.error("Error logging action:", error);
    return false;
  }
}
