# Tapple Online

Versão multiplayer para celular do jogo **Tapple**: sorteie um tema, aperte a letra
inicial da sua palavra e passe a vez antes do tempo acabar.

**Jogar:** https://madscientistbr.github.io/stop-multiplayer/

## Como jogar

1. Alguém toca em **Criar sala** e manda o link (botão **Convidar**) no grupo.
2. Cada pessoa abre o link e coloca o nome.
3. O anfitrião (quem criou, 👑) organiza a ordem dos turnos com ▲▼ ou **Embaralhar**,
   e sorteia (ou escreve) o tema.
4. Na sua vez, fale em voz alta uma palavra do tema e aperte a letra inicial dela.
   A letra trava na tela de todos e a vez passa para o próximo.
5. O tempo acabou ou você desistiu? Está fora da rodada. Quem sobrar por último ganha o ponto.
6. Achou que a palavra não vale? Toque em **Contestar** e o anfitrião decide:
   se não valer, a letra volta para o jogo e quem jogou sai.

O anfitrião pode configurar o tempo por jogada, quais letras entram no jogo e o que acontece
quando todas as letras são usadas. Também pode pausar, eliminar quem está na vez ou encerrar a rodada.

## Como funciona por dentro

- Site estático (`public/`), publicado no GitHub Pages pelo workflow `.github/workflows/pages.yml`.
- Sincronização em tempo real por um broker **MQTT público** via WebSocket seguro
  (`broker.hivemq.com` por padrão; os links com `-1` / `-2` no final usam servidores alternativos).
- O aparelho do anfitrião é o "juiz": valida as jogadas e publica o estado completo do jogo
  como mensagem *retained*. Quem entra atrasado ou recarrega a página recupera tudo na hora.
- Se o anfitrião cair, aparece para os outros o botão **Assumir**, e o jogo continua de onde parou.
- Não precisa de login. Qualquer pessoa com o código da sala consegue entrar, então não use para nada sigiloso.

Para testar com um broker local: `index.html?broker=ws://localhost:8888`.

Para adicionar temas, edite `public/temas.js`.
