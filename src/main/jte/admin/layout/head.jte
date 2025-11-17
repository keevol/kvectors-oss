@param String title

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="htmx-config" content='{"includeIndicatorStyles": false}'>
    <title>${title}</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="/js/htmx.min.js"></script>
    <script src="/js/sweetalert2.all.min.js"></script>
    <link rel="stylesheet" href="/css/fas.all.min.css">
    <link rel="stylesheet" href="/css/sweetalert2.min.css">
    @raw
    <style>

        html {
            scroll-behavior: smooth;
        }

        .htmx-indicator {
            display: none;
        }

        .htmx-request .htmx-indicator {
            display: inline;
        }

        .htmx-request.htmx-indicator {
            display: inline;
        }

        .disappear {
            animation: disappear 0s linear 3s forwards;
        }

        @keyframes disappear {
            to {
                opacity: 0;
            }
        }
        /* HTML: <div class="loader htmx-indicator" id="indicator"></div> */
        /* HTML: <div class="loader"></div> */
        .loader {
            width: 28px;
            aspect-ratio: 1;
            border-radius: 50%;
            background: #F10C49;
            animation: l2 1.5s infinite;
        }
        @keyframes l2 {
            0%,
            100%{transform:translate(-35px);box-shadow:  0     0 #F4DD51, 0     0 #E3AAD6}
            40% {transform:translate( 35px);box-shadow: -15px  0 #F4DD51,-30px  0 #E3AAD6}
            50% {transform:translate( 35px);box-shadow:  0     0 #F4DD51, 0     0 #E3AAD6}
            90% {transform:translate(-35px);box-shadow:  15px  0 #F4DD51, 30px  0 #E3AAD6}
        }

        /* HTML: <div class="slider"></div> */
        .slider {
            width: 100px;
            height: 100px;
            padding: 10px;
            box-sizing: border-box;
            display: grid;
            background: #fff;
            filter: blur(4px) contrast(10) hue-rotate(240deg);
            mix-blend-mode: darken;
        }
        .slider:before{
            content: "";
            grid-area: 1/1;
            margin: 30px 0;
            border-radius: 100px;
            background: #00ffff;
        }
        .slider:after {
            content: "";
            grid-area: 1/1;
            height: 35px;
            width: 35px;
            margin: auto;
            border-radius: 50%;
            background: #00ffff;
            animation: l5 2s infinite linear;
        }
        @keyframes l5{
            0%   {transform: translate(30px)}
            25%  {transform: translate(0)}
            50%  {transform: translate(-30px)}
            75%  {transform: translate(0)}
            100% {transform: translate(30px)}
        }
        /* HTML: <div class="spinner"></div> */
        .spinner {
            width: 50px;
            aspect-ratio: 1;
            display: grid;
        }
        .spinner::before,
        .spinner::after {
            content:"";
            grid-area: 1/1;
            --c:no-repeat radial-gradient(farthest-side,#25b09b 92%,#0000);
            background:
                    var(--c) 50%  0,
                    var(--c) 50%  100%,
                    var(--c) 100% 50%,
                    var(--c) 0    50%;
            background-size: 12px 12px;
            animation: l12 1s infinite;
        }
        .spinner::before {
            margin: 4px;
            filter: hue-rotate(45deg);
            background-size: 8px 8px;
            animation-timing-function: linear
        }

        @keyframes l12 {
            100%{transform: rotate(.5turn)}
        }

        .glow-effect {
            box-shadow: 0 0 20px rgba(59, 130, 246, 0.3);
        }
        .tech-gradient {
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        }
        .neon-border {
            border: 1px solid rgba(59, 130, 246, 0.5);
            box-shadow: 0 0 10px rgba(59, 130, 246, 0.2);
        }
        .cyber-card {
            background: linear-gradient(145deg, #1e293b, #334155);
            border: 1px solid rgba(59, 130, 246, 0.3);
        }
        .tech-input {
            background: rgba(15, 23, 42, 0.8);
            border: 1px solid rgba(59, 130, 246, 0.3);
            color: #e2e8f0;
        }
        .tech-input:focus {
            border-color: #3b82f6;
            box-shadow: 0 0 15px rgba(59, 130, 246, 0.4);
            background: rgba(15, 23, 42, 0.9);
        }
        .scrollbar-thin::-webkit-scrollbar {
            width: 4px;
        }
        .scrollbar-thin::-webkit-scrollbar-track {
            background: rgba(15, 23, 42, 0.5);
        }
        .scrollbar-thin::-webkit-scrollbar-thumb {
            background: rgba(59, 130, 246, 0.5);
            border-radius: 2px;
        }
        .scrollbar-thin::-webkit-scrollbar-thumb:hover {
            background: rgba(59, 130, 246, 0.7);
        }
    </style>
    @endraw
</head>