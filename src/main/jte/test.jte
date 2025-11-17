@import io.vertx.core.json.*

@param JsonObject json

<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes" />
    <meta author="fujohnwang" />
    <meta keywords="王福强,fujohnwang" />
    <meta description="" />
    <title>CHANGE</title>
    <style>
        ::-webkit-scrollbar {
            width: 11px;
        }
        ::-webkit-scrollbar-thumb{
            background-color: #3F83F8;
            border-radius: 3px;
        }
        html{
            scroll-behavior: smooth;
        }
        body{
            font-family: source-serif-pro, Georgia, Cambria, "Times New Roman", Times, serif;
        }
    </style>
    <link href="https://cdn.jsdelivr.net/npm/daisyui@3.5.0/dist/full.css" rel="stylesheet" type="text/css" />
    <script src="https://cdn.tailwindcss.com/"></script>
    <script src="https://unpkg.com/htmx.org@1.9.6"></script>
    <script src="https://cdn.jsdelivr.net/npm/jquery@3.7.0/dist/jquery.min.js"></script>
    <script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.11.1/dist/cdn.min.js"></script>
    <script defer src="https://cdn.jsdelivr.net/npm/@alpinejs/intersect@3.11.1/dist/cdn.min.js"></script>

    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@shoelace-style/shoelace@2.9.0/cdn/themes/light.css" />
    <script type="module" src="https://cdn.jsdelivr.net/npm/@shoelace-style/shoelace@2.9.0/cdn/shoelace-autoloader.js"></script>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/animejs/3.2.1/anime.min.js" integrity="sha512-z4OUqw38qNLpn1libAN9BsoDx6nbNFio5lA6CuTp9NlK83b89hgyCVq+N5FdBJptINztxn1Z3SaKSKUS5UP60Q==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/gsap/3.11.4/gsap.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/gsap/3.11.4/ScrollTrigger.min.js"></script>
</head>
<body>
<main class="container mx-auto p-3 space-y-3">
    <h1 class="text-6xl">Euler's Identity</h1>
    <p>In mathematics, <strong>Euler's identity</strong> is the equality</p>
    <h2 class="text-3xl">Explanation</h2>
    <p>Euler's identity is a special case of Euler's formula from complex
        analysis, which states that for any real number </p>
    <p style="font-size: 111px; color:red;">${json.getString("message")}</p>
</main>

<script>
    document.addEventListener('alpine:init', () => {
        Alpine.store('appdata', () => ({
            open: false,

            toggle() {
                this.open = ! this.open
            }
        }))
    })
</script>
</body>
</html>
